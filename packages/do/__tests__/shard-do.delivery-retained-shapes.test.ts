import { readGlobalShapeSnapshot, readShapePokeCursor, runShardMigrations } from "@lunora/shard-engine";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { DeliveryAuthorization } from "../src/delivery-authorization";
import type { ShardDOState } from "../src/shard-do";
import { Socket, TestShard } from "./_helpers/delivery-authorization";
import messagesSchema from "./_helpers/messages-schema";
import createSqliteExec from "./_helpers/node-sqlite";

class ShapeShard extends TestShard {
    public globalReads = 0;
    private readonly shapes = new Map([
        ["global", { global: true, table: "profiles" }],
        ["local", { table: "messages" }],
    ]);

    public enableAuthorization(authority: DeliveryAuthorization): void {
        // Preserve warm memos while exercising the same guard a new deployment enables.
        Reflect.set(this, "deliveryAuthorization", authority);
    }

    protected override resolveShape(name: string): { global?: boolean; table: string } | undefined {
        return this.shapes.get(name);
    }

    protected override readGlobalShapeRows(): Promise<{ doc: Record<string, unknown>; id: string }[]> {
        this.globalReads += 1;
        return Promise.resolve([{ doc: { userId: "alice" }, id: "profile" }]);
    }
}

const databases: ReturnType<typeof createSqliteExec>[] = [];

const setup = async () => {
    const database = createSqliteExec();
    databases.push(database);
    runShardMigrations(database.sql, messagesSchema, { cdc: true });
    let socket = new Socket();
    socket.attachment.connectionId = "retained-connection";
    const setAlarm = vi.fn<() => Promise<void>>(async () => {});
    const state: ShardDOState = {
        acceptWebSocket() {},
        getWebSockets: () => [socket as unknown as WebSocket],
        storage: { setAlarm, sql: database.sql as unknown as ShardDOState["storage"]["sql"] },
    };
    const shard = new ShapeShard(state, {});
    const subscribeShape = (name: string) =>
        shard.webSocketMessage(socket as unknown as WebSocket, JSON.stringify({ type: "shape_subscribe", id: name, shape: { name } }));
    await subscribeShape("local");
    await subscribeShape("global");
    await shard.webSocketMessage(
        socket as unknown as WebSocket,
        JSON.stringify({ type: "subscribe", id: "ordinary", query: { functionPath: "messages:list", args: {} } }),
    );
    const authority = vi.fn<DeliveryAuthorization>(async () => {
        return { scope: "alice", epoch: 0 };
    });
    const enable = (hibernated: boolean): ShapeShard => {
        if (hibernated) {
            const resumed = new Socket();
            resumed.attachment = structuredClone(socket.attachment);
            socket = resumed;
            return new ShapeShard(state, {}, { deliveryAuthorization: authority });
        }
        shard.enableAuthorization(authority);
        return shard;
    };
    const write = (target: ShapeShard) =>
        target.fetch(
            new Request("https://shard.internal/rpc", {
                method: "POST",
                body: JSON.stringify({ functionPath: "messages:write", args: {} }),
            }),
        );
    return { authority, database, enable, setAlarm, shard, socket: () => socket, write };
};

const refresh = async (test: Awaited<ReturnType<typeof setup>>, shard: ShapeShard): Promise<void> => {
    const response = await test.write(shard);

    expect(response.status).toBe(200);
};
const alarm = async (_test: Awaited<ReturnType<typeof setup>>, shard: ShapeShard): Promise<void> => shard.alarm();

describe("retained shapes when delivery authorization is enabled", () => {
    afterEach(() => {
        for (const database of databases.splice(0)) {
            database.close();
        }
    });

    it.each([
        { hibernated: false, path: "refresh", run: refresh },
        { hibernated: false, path: "alarm", run: alarm },
        { hibernated: true, path: "refresh", run: refresh },
        { hibernated: true, path: "alarm", run: alarm },
    ])("removes both shapes on $path with hibernated=$hibernated", async ({ hibernated, run }) => {
        expect.hasAssertions();

        const test = await setup();

        expect(readShapePokeCursor(test.database.sql, "retained-connection", "local")).toBeDefined();
        expect(readGlobalShapeSnapshot(test.database.sql, "retained-connection", "global")).toBeDefined();

        const shard = test.enable(hibernated);
        const socket = test.socket();
        socket.frames.length = 0;

        test.setAlarm.mockClear();

        const reads = shard.globalReads;

        await run(test, shard);

        expect(socket.frames.filter((frame) => frame.type === "error")).toEqual([
            expect.objectContaining({ id: "local", code: "NOT_IMPLEMENTED" }),
            expect.objectContaining({ id: "global", code: "NOT_IMPLEMENTED" }),
        ]);
        expect(socket.attachment.shapes).toEqual({});

        const caches = shard as unknown as {
            globalShapeSnapshots: WeakMap<object, Map<string, unknown>>;
            shapeMemos: WeakMap<object, Map<string, unknown>>;
        };

        expect(caches.shapeMemos.get(socket)?.size ?? 0).toBe(0);
        expect(caches.globalShapeSnapshots.get(socket)?.size ?? 0).toBe(0);
        expect(Object.keys(socket.attachment.subs)).toEqual(["ordinary"]);
        expect(readShapePokeCursor(test.database.sql, "retained-connection", "local")).toBeUndefined();
        expect(readGlobalShapeSnapshot(test.database.sql, "retained-connection", "global")).toBeUndefined();
        expect(socket.frames.some((frame) => String(frame.type).startsWith("poke"))).toBe(false);
        expect(shard.globalReads).toBe(reads);
        expect(test.setAlarm).not.toHaveBeenCalled();

        shard.rows = ["after"];

        await refresh(test, shard);

        await shard.alarm();

        expect(socket.frames.filter((frame) => frame.type === "error")).toHaveLength(2);
        expect(socket.frames.findLast((frame) => frame.type === "data")).toMatchObject({ id: "ordinary", data: ["after"] });
        expect(test.authority).toHaveBeenCalledWith(expect.objectContaining({ functionPath: "messages:list" }));
        expect(test.setAlarm).not.toHaveBeenCalled();
    });

    it("cleans retained shapes even when the socket cannot receive the error", async () => {
        expect.hasAssertions();

        const test = await setup();
        const shard = test.enable(true);
        vi.spyOn(test.socket(), "send").mockImplementation(() => {
            throw new Error("socket closed");
        });

        test.setAlarm.mockClear();

        await shard.alarm();

        expect(test.socket().attachment.shapes).toEqual({});
        expect(readShapePokeCursor(test.database.sql, "retained-connection", "local")).toBeUndefined();
        expect(readGlobalShapeSnapshot(test.database.sql, "retained-connection", "global")).toBeUndefined();
        expect(test.setAlarm).not.toHaveBeenCalled();
    });

    it("retries failed attachment cleanup without reading shape data", async () => {
        expect.hasAssertions();

        const test = await setup();
        const shard = test.enable(true);
        vi.spyOn(test.socket(), "serializeAttachment").mockImplementationOnce(() => {
            throw new Error("attachment unavailable");
        });

        test.setAlarm.mockClear();

        await shard.alarm();

        expect(Object.keys(test.socket().attachment.shapes ?? {})).toEqual(["local"]);
        expect(readShapePokeCursor(test.database.sql, "retained-connection", "local")).toBeDefined();
        expect(readGlobalShapeSnapshot(test.database.sql, "retained-connection", "global")).toBeUndefined();
        expect(test.setAlarm).toHaveBeenCalledTimes(1);

        test.setAlarm.mockClear();

        await shard.alarm();

        expect(test.socket().attachment.shapes).toEqual({});
        expect(readShapePokeCursor(test.database.sql, "retained-connection", "local")).toBeUndefined();
        expect(shard.globalReads).toBe(0);
        expect(test.setAlarm).not.toHaveBeenCalled();
    });

    it("keeps both shapes and global polling when the feature stays off", async () => {
        expect.hasAssertions();

        const test = await setup();

        test.socket().frames.length = 0;

        test.setAlarm.mockClear();

        await refresh(test, test.shard);

        await test.shard.alarm();

        expect(Object.keys(test.socket().attachment.shapes ?? {})).toEqual(["local", "global"]);
        expect(test.socket().frames.filter((frame) => frame.type === "error")).toEqual([]);
        expect(test.shard.globalReads).toBeGreaterThan(1);
        expect(test.setAlarm).toHaveBeenCalledTimes(1);
        expect(readShapePokeCursor(test.database.sql, "retained-connection", "local")).toBeDefined();
        expect(readGlobalShapeSnapshot(test.database.sql, "retained-connection", "global")).toBeDefined();
    });
});
