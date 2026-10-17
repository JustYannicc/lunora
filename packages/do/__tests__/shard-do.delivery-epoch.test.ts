import { describe, expect, it } from "vitest";

import type { ShardDOState, SubscriptionOutcome } from "../src/shard-do";
import { ShardDO } from "../src/shard-do";
import { dataFrames, gate, permit, secret, setup, Socket, TestShard } from "./_helpers/delivery-authorization";

const request = (mutationId?: string) =>
    new Request("https://shard.internal/rpc", {
        method: "POST",
        headers: { "x-lunora-userid": "alice", ...(mutationId === undefined ? {} : { "x-lunora-mutation-id": mutationId }) },
        body: JSON.stringify({ functionPath: "messages:list", args: {} }),
    });

class CachedReadShard extends ShardDO {
    public value = "old-policy";
    public runs = 0;
    // eslint-disable-next-line class-methods-use-this -- mirrors generated registry classification
    protected override isCacheableQuery(): boolean {
        return true;
    }
    public override handleRpc(): Promise<unknown> {
        this.runs += 1;
        return Promise.resolve(this.value);
    }
}

class TransactionalReceiptShard extends ShardDO {
    public fail = false;
    public runs = 0;
    // eslint-disable-next-line class-methods-use-this -- mirrors generated registry classification
    protected override isMutationFunction(): boolean {
        return true;
    }
    public override handleRpc(): Promise<unknown> {
        return this.runInTransaction(() => {
            this.runs += 1;
            this.commitMutationBookkeeping({ runs: this.runs });
            if (this.fail) {
                throw new Error("rollback after bookkeeping");
            }
            return { runs: this.runs };
        });
    }
}

class InterleavedRefreshShard extends TestShard {
    public readonly oldStarted = gate();
    public readonly newFinished = gate();
    public readonly releaseOld = gate();
    protected override async executeSubscription(): Promise<SubscriptionOutcome> {
        this.queryCalls += 1;
        const result = [...this.rows];
        if (this.queryCalls === 3) {
            this.oldStarted.resolve();
            await this.releaseOld.promise;
        } else if (this.queryCalls === 4) {
            this.newFinished.resolve();
        }
        return { result, tables: new Set(["messages"]) };
    }
}

class UnrelatedWriteShard extends TestShard {
    public override handleRpc(): Promise<unknown> {
        this.recordChangedTable("unrelated");
        return Promise.resolve("ok");
    }
}

describe("delivery authority epoch", () => {
    it("keeps the delivered generation while a new epoch resets its diff baseline", async () => {
        const test = setup();
        const releaseOld = gate();
        test.shard.queryGate = releaseOld.promise;
        const oldSeed = test.send();
        await test.shard.queryStarted.promise;
        test.shard.queryGate = undefined;
        test.shard.rows = ["middle"];
        await test.write();
        expect(dataFrames(test.socket).map((frame) => frame.data)).toEqual([["middle"]]);
        test.regrant();
        const releaseNew = gate();
        test.shard.queryStarted = gate();
        test.shard.queryGate = releaseNew.promise;
        test.shard.rows = ["latest"];
        const newRefresh = test.write();
        await test.shard.queryStarted.promise;
        releaseOld.resolve();
        await oldSeed;
        expect(dataFrames(test.socket).map((frame) => frame.data)).toEqual([["middle"]]);
        releaseNew.resolve();
        await newRefresh;
        expect(dataFrames(test.socket).map((frame) => frame.data)).toEqual([["middle"], ["latest"]]);
        expect(dataFrames(test.socket).map((frame) => frame.type)).toEqual(["data", "data"]);
    });

    it("reauthorizes and replaces the memo even when the write misses the old read set", async () => {
        const test = setup();
        const shard = new UnrelatedWriteShard(test.state, {}, { deliveryAuthorization: test.authority });
        await test.send(shard);
        test.regrant();
        shard.rows = ["new-policy"];
        await shard.fetch(request());
        expect(shard.queryCalls).toBe(2);
        expect(dataFrames(test.socket).at(-1)).toMatchObject({ type: "data", data: ["new-policy"] });
    });

    it("bypasses a configured native RPC cache when current policy changes without table writes", async () => {
        const test = setup();
        const shard = new CachedReadShard(test.state, {}, { reactiveCache: {}, deliveryAuthorization: test.authority });
        expect(await (await shard.fetch(request())).json()).toMatchObject({ result: "old-policy" });
        test.regrant();
        shard.value = "new-policy";
        expect(await (await shard.fetch(request())).json()).toMatchObject({ result: "new-policy" });
        expect(shard.runs).toBe(2);
    });

    it("does not share an in-flight refresh result across authority epochs", async () => {
        const test = setup();
        const second = new Socket();
        const shard = new InterleavedRefreshShard(
            { ...test.state, getWebSockets: () => [test.socket, second] as unknown as WebSocket[] },
            { LUNORA_RELAY_SECRET: secret },
            { deliveryAuthorization: test.authority },
        );
        const subscribe = (socket: Socket) =>
            shard.webSocketMessage(
                socket as unknown as WebSocket,
                JSON.stringify({ type: "subscribe", id: "view", query: { functionPath: "messages:list", args: {} } }),
            );
        await subscribe(test.socket);
        await subscribe(second);
        let refreshAuthorityCalls = 0;
        test.authority.mockImplementation(async ({ transport }) => {
            if (transport === "rpc") {
                return permit(0);
            }
            refreshAuthorityCalls += 1;
            if (refreshAuthorityCalls === 1) {
                return permit(0);
            }
            await shard.oldStarted.promise;
            shard.rows = ["new-policy"];
            return permit(1);
        });
        const refresh = shard.fetch(request());
        await shard.newFinished.promise;
        shard.releaseOld.resolve();
        await refresh;
        expect(shard.queryCalls).toBe(4);
        expect(dataFrames(second).at(-1)).toMatchObject({ type: "data", data: ["new-policy"] });
    });

    it("commits the original permit with its native receipt and rolls both back together", async () => {
        const test = setup();
        const state = {
            ...test.state,
            storage: {
                ...test.state.storage,
                transaction: async <T>(run: () => Promise<T>): Promise<T> => {
                    test.database.sql.exec("BEGIN");
                    try {
                        const value = await run();
                        test.database.sql.exec("COMMIT");
                        return value;
                    } catch (error) {
                        test.database.sql.exec("ROLLBACK");
                        throw error;
                    }
                },
            },
        } as ShardDOState;
        const shard = new TransactionalReceiptShard(state, {}, { deliveryAuthorization: test.authority });
        shard.fail = true;
        expect((await shard.fetch(request("operation"))).status).toBe(500);
        expect(test.database.raw("SELECT * FROM __idempotency")).toEqual([]);
        expect(test.database.raw("SELECT * FROM __lunora_delivery_receipt")).toEqual([]);
        shard.fail = false;
        expect((await shard.fetch(request("operation"))).status).toBe(200);
        expect(test.database.raw("SELECT scope, epoch FROM __lunora_delivery_receipt")).toEqual([permit(0)]);
        expect((await shard.fetch(request("operation"))).status).toBe(200);
        expect(shard.runs).toBe(2);
        test.regrant();
        const replay = await shard.fetch(request("operation"));
        expect(replay.status).toBe(409);
        expect(await replay.json()).toMatchObject({ error: { code: "DELIVERY_RECEIPT_FENCED" } });
        expect(shard.runs).toBe(2);
    });
});
