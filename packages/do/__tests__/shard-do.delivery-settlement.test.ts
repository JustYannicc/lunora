import type { DatabaseWriterLike, SqlExec } from "@lunora/shard-engine";
import { createShardCtxDb, readCdcCursor } from "@lunora/shard-engine";
import { afterEach, describe, expect, it, vi } from "vitest";

import { applyOptimisticLayer, dropConfirmedLayers, refoldSubscription } from "../../client/src/optimistic-layers";
import type { SubscriptionState } from "../../client/src/subscription";
import { cleanupDatabases, setup, TestShard } from "./_helpers/delivery-authorization";
import messagesSchema from "./_helpers/messages-schema";

class UnrelatedCdcShard extends TestShard {
    private writer: DatabaseWriterLike | undefined;
    private readonly mutators = new Set(["rooms:tick"]);
    public override handleRpc(path: string): Promise<unknown> {
        return this.runInTransaction(async () => {
            this.writer ??= createShardCtxDb({ sql: this.sql as SqlExec, schema: messagesSchema, cdc: true, broadcast: () => {} });
            const result = path === "rooms:join" ? await this.writer.insert("roomMembers", { roomId: "r1", userId: "alice" }) : "ok";
            this.recordChangedTable("roomMembers");
            this.commitMutationBookkeeping(result);
            return result;
        });
    }
    protected override isCustomMutator(path: string): boolean {
        return this.mutators.has(path);
    }
}

const clientState = (): SubscriptionState => {
    return {
        acked: true,
        args: {},
        argsKey: "{}",
        callbacks: new Set(),
        checkpointCallbacks: new Set(),
        errorCallbacks: new Set(),
        fn: { __lunoraRef: "messages:list" },
        id: "view",
        lastValue: ["before"],
        optimisticLayers: [],
        serverBase: ["before"],
        wireArgs: {},
    };
};

const write = (shard: UnrelatedCdcShard, path = "rooms:join", headers: Record<string, string> = {}) =>
    shard.fetch(
        new Request("https://shard.internal/rpc", {
            method: "POST",
            body: JSON.stringify({ functionPath: path, args: {} }),
            headers: { "x-lunora-userid": "alice", "x-lunora-mutation-id": crypto.randomUUID(), ...headers },
        }),
    );

const readCommitCursor = async (response: Response): Promise<number> => {
    const body: unknown = await response.json();
    if (!body || typeof body !== "object" || !("commitCursor" in body) || typeof body.commitCursor !== "number") {
        throw new Error("Mutation response has no commit cursor");
    }
    return body.commitCursor;
};

const setupWatermark = (cdc: boolean) => {
    const test = setup();
    if (!cdc) {
        test.database.sql.exec("DROP TABLE __cdc_log");
    }
    return test;
};

describe("delivery-authorized unchanged subscription settlement", () => {
    afterEach(cleanupDatabases);

    it.each([true, false])("settles a watermark without cursor progress and skips repeated flushes with CDC=%s", async (cdc) => {
        expect.hasAssertions();

        const test = setupWatermark(cdc);

        test.socket.attachment.clientId = "client";

        const shard = new UnrelatedCdcShard(test.state, {}, { deliveryAuthorization: test.authority });

        await test.send(shard);

        test.socket.frames.length = 0;

        const response = await write(shard, "rooms:tick", { "x-lunora-client-id": "client", "x-lunora-client-seq": "1" });

        await expect(response.json()).resolves.toMatchObject({ lastMutationId: 1 });
        expect(test.socket.frames).toEqual([expect.objectContaining({ type: "settled", id: "view", lastMutationId: 1 })]);
        expect(test.socket.frames[0]?.cursor).toBe(cdc ? 0 : undefined);

        await write(shard, "rooms:notify");

        expect(test.socket.frames).toHaveLength(1);
        expect(shard.queryCalls).toBe(1);
    });

    it("does not settle or advance a denied subscription after a committed unrelated write", async () => {
        expect.hasAssertions();

        const test = setup();
        const shard = new UnrelatedCdcShard(test.state, {}, { deliveryAuthorization: test.authority });

        await test.send(shard);

        const client = clientState();
        const layer = applyOptimisticLayer(client, () => ["optimistic"]);

        test.socket.frames.length = 0;

        test.authority.mockImplementation(async ({ transport }) => (transport === "rpc" ? { scope: "opaque-authority-scope", epoch: 0 } : undefined));

        const response = await write(shard);
        const cursor = await readCommitCursor(response);
        layer?.confirm(cursor);

        expect(cursor).toBeGreaterThan(0);
        expect(client.optimisticLayers).toHaveLength(1);
        expect(client.serverCursor).toBeUndefined();
        expect(test.socket.frames).toEqual([expect.objectContaining({ type: "error", error: { code: "FORBIDDEN", message: "AccessDenied" } })]);
        expect(test.socket.attachment.subs).toEqual({});
        expect(shard.queryCalls).toBe(1);
    });

    it("retries a failed settlement without advancing its memo or rerunning the query", async () => {
        expect.hasAssertions();

        const test = setup();
        const shard = new UnrelatedCdcShard(test.state, {}, { deliveryAuthorization: test.authority });

        await test.send(shard);

        test.socket.frames.length = 0;

        vi.spyOn(test.socket, "send").mockImplementationOnce(() => {
            throw new Error("socket buffer unavailable");
        });
        const response = await write(shard);
        const cursor = await readCommitCursor(response);

        expect(test.socket.frames).toEqual([]);

        await write(shard, "rooms:notify");

        expect(test.socket.frames).toEqual([expect.objectContaining({ type: "settled", id: "view", cursor })]);

        await write(shard, "rooms:notify");

        expect(test.socket.frames).toHaveLength(1);
        expect(shard.queryCalls).toBe(1);
    });

    it("clears the actual confirmed optimistic layer after an unrelated CDC write without rerunning the query", async () => {
        expect.hasAssertions();

        const test = setup();
        const shard = new UnrelatedCdcShard(test.state, {}, { deliveryAuthorization: test.authority });

        await test.send(shard);

        const client = clientState();
        const layer = applyOptimisticLayer(client, () => ["optimistic"]);
        const response = await write(shard);
        const cursor = await readCommitCursor(response);

        expect(response.status).toBe(200);
        expect(cursor).toBeGreaterThan(0);
        expect(cursor).toBe(readCdcCursor(test.database.sql));

        layer?.confirm(cursor);

        expect(client.optimisticLayers).toHaveLength(1);

        for (const frame of test.socket.frames.filter((candidate) => candidate.type === "settled")) {
            const settledCursor = frame.cursor as number;
            client.serverCursor = settledCursor;
            dropConfirmedLayers(client, settledCursor);
            refoldSubscription(client, client.serverBase);
        }

        expect(client.optimisticLayers).toEqual([]);
        expect(client.lastValue).toEqual(["before"]);
        expect(shard.queryCalls).toBe(1);
        expect(test.socket.frames.filter((frame) => frame.type === "settled")).toEqual([
            expect.objectContaining({ id: "view", cursor, epoch: expect.any(String) }),
        ]);
    });
});
