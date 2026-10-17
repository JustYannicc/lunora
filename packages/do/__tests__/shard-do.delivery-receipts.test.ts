import { describe, expect, it, vi } from "vitest";

import { DISPATCH_CLAIM_CEILING_MS } from "../../../shared/dispatch-claim";
import type { ShardDOState } from "../src/shard-do";
import { gate, permit, setup, TestShard } from "./_helpers/delivery-authorization";

const request = (mutationId = "operation") =>
    new Request("https://shard.internal/rpc", {
        method: "POST",
        headers: { "x-lunora-userid": "alice", "x-lunora-mutation-id": mutationId },
        body: JSON.stringify({ functionPath: "messages:act", args: {} }),
    });

class TwoRunShard extends TestShard {
    public readonly first = { entered: gate(), release: gate(), value: "old-policy" };
    public readonly second = { entered: gate(), release: gate(), value: "new-policy" };
    public override async handleRpc(): Promise<unknown> {
        const turn = this.rpcRuns === 0 ? this.first : this.second;
        this.rpcRuns += 1;
        turn.entered.resolve();
        await turn.release.promise;
        return turn.value;
    }
}

class ReceiptMutationShard extends TestShard {
    public writeEffect = () => {};
    // eslint-disable-next-line class-methods-use-this -- mirrors generated registry classification
    protected override isMutationFunction(): boolean {
        return true;
    }
    public override handleRpc(): Promise<unknown> {
        return this.runInTransaction(() => {
            this.rpcRuns += 1;
            this.writeEffect();
            const result = { runs: this.rpcRuns };
            this.commitMutationBookkeeping(result);
            return result;
        });
    }
}

const transactionalState = (test: ReturnType<typeof setup>): ShardDOState =>
    ({
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
    }) as ShardDOState;

describe("delivery-authorized native receipts", () => {
    it.each([
        { failure: "old marker", first: "old", table: "__lunora_delivery_receipt", condition: "NEW.epoch = 0" },
        { failure: "old row", first: "old", table: "__idempotency", condition: `NEW.result_json = '\"old-policy\"'` },
        { failure: "new marker", first: "new", table: "__lunora_delivery_receipt", condition: "NEW.epoch = 1" },
        { failure: "new row", first: "new", table: "__idempotency", condition: `NEW.result_json = '\"new-policy\"'` },
        { failure: "new marker", first: "old", table: "__lunora_delivery_receipt", condition: "NEW.epoch = 1" },
        { failure: "new row", first: "old", table: "__idempotency", condition: `NEW.result_json = '\"new-policy\"'` },
        { failure: "old marker", first: "new", table: "__lunora_delivery_receipt", condition: "NEW.epoch = 0" },
        { failure: "old row", first: "new", table: "__idempotency", condition: `NEW.result_json = '\"old-policy\"'` },
    ])("never leaks older bytes when $failure fails and $first completes first", async ({ first: firstCompletion, table, condition }) => {
        const test = setup();
        const shard = new TwoRunShard(test.state, {}, { deliveryAuthorization: test.authority });
        let now = 1_000_000;
        const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
        try {
            const first = shard.fetch(request());
            await shard.first.entered.promise;
            test.database.sql.exec(
                `CREATE TRIGGER reject_insert BEFORE INSERT ON ${table} WHEN ${condition} BEGIN SELECT RAISE(FAIL, 'insert unavailable'); END`,
            );
            now += DISPATCH_CLAIM_CEILING_MS + 1;
            test.regrant();
            const second = shard.fetch(request());
            await shard.second.entered.promise;
            const earlier = firstCompletion === "old" ? { turn: shard.first, response: first } : { turn: shard.second, response: second };
            const later = firstCompletion === "old" ? { turn: shard.second, response: second } : { turn: shard.first, response: first };
            earlier.turn.release.resolve();
            await earlier.response;
            now += 1;
            later.turn.release.resolve();
            await later.response;
            const replay = await shard.fetch(request());
            const body = await replay.json();
            expect([
                { status: 200, body: expect.objectContaining({ result: "new-policy" }) },
                { status: 409, body: expect.objectContaining({ error: expect.objectContaining({ code: "DELIVERY_RECEIPT_FENCED" }) }) },
            ]).toContainEqual({ status: replay.status, body });
            expect(shard.rpcRuns).toBe(2);
        } finally {
            shard.first.release.resolve();
            shard.second.release.resolve();
            clock.mockRestore();
        }
    });

    it("keeps the winning receipt epoch when an expired claim permits a second ungated run", async () => {
        const test = setup();
        const shard = new TwoRunShard(test.state, {}, { deliveryAuthorization: test.authority });
        let now = 1_000_000;
        const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
        try {
            const first = shard.fetch(request());
            await shard.first.entered.promise;
            now += DISPATCH_CLAIM_CEILING_MS + 1;
            test.regrant();
            const second = shard.fetch(request());
            await shard.second.entered.promise;
            shard.first.release.resolve();
            expect((await first).status).toBe(200);
            shard.second.release.resolve();
            expect((await second).status).toBe(200);
            expect(test.database.raw("SELECT result_json FROM __idempotency")).toEqual([{ result_json: '"old-policy"' }]);
            expect(test.database.raw("SELECT scope, epoch FROM __lunora_delivery_receipt")).toEqual([permit(0)]);
            const replay = await shard.fetch(request());
            expect(replay.status).toBe(409);
            expect(await replay.json()).toMatchObject({ error: { code: "DELIVERY_RECEIPT_FENCED" } });
            expect(shard.rpcRuns).toBe(2);
        } finally {
            shard.first.release.resolve();
            shard.second.release.resolve();
            clock.mockRestore();
        }
    });

    it("never certifies an older run's bytes after its receipt marker insert fails", async () => {
        const test = setup();
        const shard = new TwoRunShard(test.state, {}, { deliveryAuthorization: test.authority });
        let now = 1_000_000;
        const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
        try {
            const first = shard.fetch(request());
            await shard.first.entered.promise;
            test.database.sql.exec(
                "CREATE TRIGGER reject_old_marker BEFORE INSERT ON __lunora_delivery_receipt WHEN NEW.epoch = 0 BEGIN SELECT RAISE(FAIL, 'marker unavailable'); END",
            );
            now += DISPATCH_CLAIM_CEILING_MS + 1;
            test.regrant();
            const second = shard.fetch(request());
            await shard.second.entered.promise;
            shard.first.release.resolve();
            expect((await first).status).toBe(500);
            shard.second.release.resolve();
            expect((await second).status).toBe(200);
            const replay = await shard.fetch(request());
            expect(replay.status).toBe(409);
            expect(await replay.json()).toMatchObject({ error: { code: "DELIVERY_RECEIPT_FENCED" } });
            expect(test.database.raw("SELECT result_json FROM __idempotency")).toEqual([{ result_json: '"old-policy"' }]);
            expect(test.database.raw("SELECT scope, epoch FROM __lunora_delivery_receipt")).toEqual([]);
            expect(shard.rpcRuns).toBe(2);
        } finally {
            shard.first.release.resolve();
            shard.second.release.resolve();
            clock.mockRestore();
        }
    });

    it("fences a legacy receipt without a permit marker and never reruns it", async () => {
        const test = setup();
        const legacy = new TestShard(test.state, {});
        expect((await legacy.fetch(request())).status).toBe(200);
        const guarded = new TestShard(test.state, {}, { deliveryAuthorization: test.authority });
        const replay = await guarded.fetch(request());
        expect(replay.status).toBe(409);
        expect(await replay.json()).toMatchObject({ error: { code: "DELIVERY_RECEIPT_FENCED" } });
        expect(legacy.rpcRuns).toBe(1);
        expect(guarded.rpcRuns).toBe(0);
    });

    it("replays current-epoch ungated results and expires their markers with their native rows", async () => {
        const test = setup();
        const shard = new TestShard(test.state, {}, { deliveryAuthorization: test.authority });
        let now = 100_000_000;
        const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
        try {
            const first = await shard.fetch(request("old"));
            const firstBody = await first.json();
            expect(await (await shard.fetch(request("old"))).json()).toEqual(firstBody);
            expect(shard.rpcRuns).toBe(1);
            now += 2 * 86_400_000;
            expect((await shard.fetch(request("fresh"))).status).toBe(200);
            expect(test.database.raw("SELECT mutation_id FROM __idempotency")).toEqual([{ mutation_id: "fresh" }]);
            expect(test.database.raw("SELECT mutation_id FROM __lunora_delivery_receipt")).toEqual([{ mutation_id: "fresh" }]);
        } finally {
            clock.mockRestore();
        }
    });

    it.each(["__idempotency", "__lunora_delivery_receipt"])("commits user writes and receipt markers when %s retention fails", async (table) => {
        const test = setup();
        test.database.sql.exec("CREATE TABLE receipt_effects (value INTEGER NOT NULL)");
        test.database.sql.exec("INSERT INTO receipt_effects(value) VALUES (0)");
        const shard = new ReceiptMutationShard(transactionalState(test), {}, { deliveryAuthorization: test.authority });
        shard.writeEffect = () => {
            test.database.sql.exec("UPDATE receipt_effects SET value = value + 1");
        };
        let now = 100_000_000;
        const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
        try {
            expect((await shard.fetch(request("old"))).status).toBe(200);
            test.database.sql.exec(`CREATE TRIGGER reject_receipt_trim BEFORE DELETE ON ${table} BEGIN SELECT RAISE(FAIL, 'retention unavailable'); END`);
            now += 2 * 86_400_000;
            expect((await shard.fetch(request("fresh"))).status).toBe(200);
            expect(test.database.raw("SELECT value FROM receipt_effects")).toEqual([{ value: 2 }]);
            expect(test.database.raw("SELECT scope, epoch FROM __lunora_delivery_receipt WHERE mutation_id = 'fresh'")).toEqual([permit(0)]);
            expect((await shard.fetch(request("fresh"))).status).toBe(200);
            expect(test.database.raw("SELECT value FROM receipt_effects")).toEqual([{ value: 2 }]);
        } finally {
            clock.mockRestore();
        }
    });
});
