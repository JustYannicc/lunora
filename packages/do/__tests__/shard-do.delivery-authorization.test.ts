import type { SocketAttachment, SubscriptionIdentity } from "@lunora/shard-engine";
import { readCdcEpoch, runShardMigrations } from "@lunora/shard-engine";
import { afterEach, describe, expect, it, vi } from "vitest";

import { signSiblingBody } from "../../shard-engine/src/sibling-channel";
import { installDeliveryFence } from "../src/delivery-authorization";
import type { DeliveryAuthorization, DeliveryPermit } from "../src/delivery-authorization";
import type { ShardDOState, SubscriptionOutcome } from "../src/shard-do";
import { ShardDO } from "../src/shard-do";
import messagesSchema from "./_helpers/messages-schema";
import createSqliteExec from "./_helpers/node-sqlite";

const drain = vi.hoisted(() => ({ run: async (): Promise<void> => {} }));
vi.mock("@lunora/shard-engine", async (original) => ({
    ...(await original<typeof import("@lunora/shard-engine")>()),
    awaitWsDrain: () => drain.run(),
}));

const secret = "test-internal-control-secret";
const permit = (epoch: number): DeliveryPermit => ({ scope: "opaque-authority-scope", epoch });
const gate = () => Promise.withResolvers<void>();

class Socket {
    public attachment: SocketAttachment = { subs: {}, identity: { subject: "alice" }, userId: "alice" };
    public readonly frames: Array<Record<string, unknown>> = [];
    public deserializeAttachment(): SocketAttachment {
        return this.attachment;
    }
    public serializeAttachment(attachment: SocketAttachment): void {
        this.attachment = attachment;
    }
    public send(frame: string): void {
        this.frames.push(JSON.parse(frame));
    }
}

class TestShard extends ShardDO {
    public rows = ["before"];
    public queryStarted = gate();
    public queryGate: Promise<void> | undefined;
    public queryCalls = 0;
    public observedIdentity: SubscriptionIdentity | undefined;
    public override async handleRpc(): Promise<unknown> {
        this.recordChangedTable("messages");
        return { result: "receipt-payload" };
    }
    protected override async executeSubscription(_path: string, _args: Record<string, unknown>, identity?: SubscriptionIdentity): Promise<SubscriptionOutcome> {
        this.queryCalls += 1;
        this.observedIdentity = identity;
        const rows = [...this.rows];
        this.queryStarted.resolve();
        await this.queryGate;
        return { result: rows, tables: new Set(["messages"]) };
    }
}

const databases: ReturnType<typeof createSqliteExec>[] = [];
afterEach(() => {
    drain.run = async () => {};
    for (const database of databases.splice(0)) {
        database.close();
    }
});

const setup = () => {
    const database = createSqliteExec();
    databases.push(database);
    runShardMigrations(database.sql, messagesSchema, { cdc: true });
    let allowed = true;
    let epoch = 0;
    const authority = vi.fn<DeliveryAuthorization>(async () => (allowed ? permit(epoch) : undefined));
    const socket = new Socket();
    const state: ShardDOState = {
        acceptWebSocket() {},
        getWebSockets: () => [socket as unknown as WebSocket],
        storage: { sql: database.sql as unknown as ShardDOState["storage"]["sql"] },
    };
    const makeShard = () => new TestShard(state, { LUNORA_RELAY_SECRET: secret }, { deliveryAuthorization: authority });
    const shard = makeShard();
    const send = (target = shard, sinceSeq?: number) =>
        target.webSocketMessage(
            socket as unknown as WebSocket,
            JSON.stringify({
                type: "subscribe",
                id: "view",
                query: { functionPath: "messages:list", args: {}, sinceSeq, sinceEpoch: readCdcEpoch(database.sql) },
            }),
        );
    const fence = async (target = shard, nextEpoch = 1, signed = true) => {
        const raw = JSON.stringify({ type: "delivery-fence", fence: permit(nextEpoch) });
        return target.fetch(
            new Request("https://shard.internal/_lunora/delivery-fence", {
                method: "POST",
                body: raw,
                headers: signed ? { "x-lunora-relay-sig": await signSiblingBody(secret, raw) } : {},
            }),
        );
    };
    const write = (operationKey?: string) =>
        shard.fetch(
            new Request("https://shard.internal/rpc", {
                method: "POST",
                body: JSON.stringify({ functionPath: "messages:write", args: {} }),
                headers: {
                    "x-lunora-userid": "alice",
                    "x-lunora-identity": JSON.stringify({ subject: "alice" }),
                    ...(operationKey === undefined ? {} : { "x-lunora-mutation-id": operationKey }),
                },
            }),
        );
    return {
        database,
        authority,
        socket,
        shard,
        makeShard,
        send,
        fence,
        write,
        revoke() {
            allowed = false;
            epoch += 1;
        },
        regrant() {
            allowed = true;
            epoch += 1;
        },
    };
};

const dataFrames = (socket: Socket) => socket.frames.filter((frame) => ["data", "delta", "resume", "settled"].includes(String(frame.type)));

describe("delivery authorization", () => {
    it("fences a seed paused inside its query without advancing its memo", async () => {
        const test = setup();
        const blocked = gate();
        test.shard.queryGate = blocked.promise;
        const seed = test.send();
        await test.shard.queryStarted.promise;
        test.revoke();
        expect((await test.fence()).status).toBe(204);
        blocked.resolve();
        await seed;
        expect(dataFrames(test.socket)).toEqual([]);
        expect(test.shard.observedIdentity).toMatchObject({ userId: "alice", identity: { subject: "alice" } });
        test.regrant();
        await test.write();
        expect(dataFrames(test.socket).map((frame) => frame.type)).toEqual(["data"]);
    });

    it("checks the fence after backpressure drains and keeps the last delivered baseline", async () => {
        const test = setup();
        await test.send();
        const entered = gate();
        const release = gate();
        drain.run = async () => {
            entered.resolve();
            await release.promise;
        };
        test.shard.rows = ["after"];
        const refresh = test.write();
        await entered.promise;
        test.revoke();
        await test.fence();
        release.resolve();
        const response = await refresh;
        expect(response.status).toBe(409);
        expect(await response.json()).toMatchObject({ error: { code: "DELIVERY_FENCED" } });
        expect(dataFrames(test.socket).map((frame) => frame.data)).toEqual([["before"]]);
        test.regrant();
        await test.write();
        expect(dataFrames(test.socket).map((frame) => frame.type)).toEqual(["data", "data"]);
        expect(dataFrames(test.socket).at(-1)?.data).toEqual(["after"]);
    });

    it("reauthorizes old-cursor reconnect before query or cached resume, including a fresh instance", async () => {
        const test = setup();
        await test.send();
        test.revoke();
        await test.fence();
        test.socket.frames.length = 0;
        const woke = test.makeShard();
        await test.send(woke, 0);
        expect(woke.queryCalls).toBe(0);
        expect(dataFrames(test.socket)).toEqual([]);
        expect(test.socket.frames.at(-1)).toMatchObject({ type: "error", error: { code: "FORBIDDEN", message: "AccessDenied" } });
    });

    it("notifies an idle hibernated subscription immediately, retaining only its opaque marker", async () => {
        const test = setup();
        await test.send();
        expect(test.socket.attachment.deliveryPermits).toEqual({ view: permit(0) });
        test.socket.frames.length = 0;
        const woke = test.makeShard();
        await test.fence(woke);
        expect(test.socket.frames).toEqual([{ type: "error", id: "view", code: "FORBIDDEN", error: { code: "FORBIDDEN", message: "AccessDenied" } }]);
        await woke.webSocketMessage(test.socket as unknown as WebSocket, JSON.stringify({ type: "unsubscribe", id: "view" }));
        expect(test.socket.attachment.deliveryPermits).toEqual({});
    });

    it("leaves unrelated scopes subscribed and rejects malformed fence epochs", async () => {
        const test = setup();
        test.authority.mockResolvedValue({ scope: "unrelated", epoch: 0 });
        await test.send();
        test.socket.frames.length = 0;
        await test.fence();
        expect(test.socket.frames).toEqual([]);
        expect((await test.fence(test.shard, -1)).status).toBe(400);
        expect((await test.fence(test.shard, 1.5)).status).toBe(400);
        expect((await test.fence(test.shard, Number.MAX_SAFE_INTEGER + 1)).status).toBe(400);
    });

    it("authorizes a successful old-cursor resume without replacing its cached value", async () => {
        const test = setup();
        await test.send();
        test.socket.frames.length = 0;
        await test.send(test.makeShard(), 0);
        expect(dataFrames(test.socket).map((frame) => frame.type)).toEqual(["resume"]);
        expect(test.authority).toHaveBeenCalledTimes(2);
    });

    it("persists monotonic fences across construction and rejects stale authority permits", async () => {
        const test = setup();
        await test.fence(test.shard, 2);
        await test.fence(test.shard, 1);
        const woke = test.makeShard();
        await test.send(woke);
        expect(woke.queryCalls).toBe(0);
        expect(dataFrames(test.socket)).toEqual([]);
        expect(test.database.raw("SELECT epoch FROM __lunora_delivery_fence")).toEqual([{ epoch: 2 }]);
    });

    it("installs through the server-only signed binding helper and propagates rejection", async () => {
        const test = setup();
        await installDeliveryFence(
            { fetch: (url, init) => test.shard.fetch(new Request(url, init)) },
            {
                env: { LUNORA_RELAY_SECRET: secret },
                shardKey: undefined,
                fence: permit(4),
            },
        );
        expect(test.database.raw("SELECT epoch FROM __lunora_delivery_fence")).toEqual([{ epoch: 4 }]);
        await expect(
            installDeliveryFence(
                { fetch: async () => new Response(undefined, { status: 503 }) },
                {
                    env: { LUNORA_RELAY_SECRET: secret },
                    shardKey: undefined,
                    fence: permit(5),
                },
            ),
        ).rejects.toThrow("not acknowledged");
    });

    it("requires authenticated control requests and never installs an anonymous fence", async () => {
        const test = setup();
        expect((await test.fence(test.shard, 1, false)).status).toBe(403);
        await test.send();
        expect(dataFrames(test.socket)).toHaveLength(1);
        const noSecret = new TestShard(
            { acceptWebSocket() {}, getWebSockets: () => [], storage: { sql: test.database.sql as unknown as ShardDOState["storage"]["sql"] } },
            {},
            { deliveryAuthorization: test.authority },
        );
        expect((await test.fence(noSecret)).status).toBe(403);
    });

    it.each(["stream", "shape_subscribe", "whisper", "whisper_subscribe"])("refuses unsupported %s without invoking a query", async (type) => {
        const test = setup();
        await test.shard.webSocketMessage(test.socket as unknown as WebSocket, JSON.stringify({ type, id: "unsupported", functionPath: "messages:list" }));
        expect(test.shard.queryCalls).toBe(0);
        expect(test.socket.frames).toEqual([expect.objectContaining({ type: "error", code: "FORBIDDEN" })]);
    });

    it("reauthorizes repeated RPC reads before any handler or receipt response", async () => {
        const test = setup();
        const rpc = vi.spyOn(test.shard, "handleRpc");
        expect((await test.write("same-operation")).status).toBe(200);
        expect((await test.write("same-operation")).status).toBe(200);
        test.revoke();
        expect((await test.write("same-operation")).status).toBe(403);
        expect(rpc).toHaveBeenCalledTimes(1);
        test.regrant();
        expect(await (await test.write("same-operation")).json()).toEqual({ commitCursor: 0, result: { result: "receipt-payload" } });
        expect(rpc).toHaveBeenCalledTimes(1);
    });
});
