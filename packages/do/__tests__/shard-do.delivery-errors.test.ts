import { LunoraError } from "@lunora/errors";
import { afterEach, describe, expect, it } from "vitest";

import { cleanupDatabases, dataFrames, gate, permit, setup } from "./_helpers/delivery-authorization";

const failures = [
    { name: "coded denial", error: new LunoraError("FORBIDDEN", "policy refused", { data: { reason: "role" } }), status: 403, code: "FORBIDDEN" },
    { name: "callback type error", error: new TypeError("private implementation detail"), status: 500, code: "INTERNAL" },
    { name: "transient outage", error: new Error("authority offline"), status: 503, code: "SHARD_UNAVAILABLE" },
];

const permanentFailures = [
    {
        code: "FORBIDDEN",
        run: async () => {
            throw new LunoraError("FORBIDDEN", "policy refused");
        },
    },
    { code: "INTERNAL", run: async () => permit(-1) },
];

describe("delivery authority failures", () => {
    afterEach(cleanupDatabases);

    it.each(failures)("maps $name without running the RPC", async ({ error, status, code }) => {
        expect.hasAssertions();

        const test = setup();

        test.authority.mockRejectedValue(error);

        const response = await test.write();

        expect(response.status).toBe(status);
        await expect(response.json()).resolves.toMatchObject({ error: { code } });
        expect(test.shard.rpcRuns).toBe(0);
    });

    it("preserves a coded denial's public message and data", async () => {
        expect.hasAssertions();

        const test = setup();

        test.authority.mockRejectedValue(failures[0]?.error);

        const response = await test.write();

        await expect(response.json()).resolves.toMatchObject({ error: { code: "FORBIDDEN", message: "policy refused", data: { reason: "role" } } });
    });

    it("reports an invalid permit as a redacted terminal INTERNAL verdict", async () => {
        expect.hasAssertions();

        const test = setup();

        test.authority.mockResolvedValue(permit(-1));

        const response = await test.write();

        expect(response.status).toBe(500);
        await expect(response.json()).resolves.toMatchObject({ error: { code: "INTERNAL", message: "internal error" } });
        expect(test.shard.rpcRuns).toBe(0);
    });

    it.each(permanentFailures)("settles a subscription seed's $code authority failure", async ({ code, run }) => {
        expect.hasAssertions();

        const test = setup();

        test.authority.mockImplementation(run);

        await test.send();

        expect(test.socket.frames.at(-1)).toMatchObject({ type: "error", error: { code } });
        expect(dataFrames(test.socket)).toEqual([]);
        expect(test.shard.queryCalls).toBe(0);
    });

    it.each(permanentFailures)("clears a live subscription after a permanent $code authority failure", async ({ code, run }) => {
        expect.hasAssertions();

        const test = setup();

        await test.send();

        test.authority.mockImplementation(async ({ transport }) => (transport === "subscription" ? run() : permit(0)));

        await test.write();

        expect(test.socket.frames.at(-1)).toMatchObject({ type: "error", error: { code } });
        expect(test.socket.attachment.subs).toEqual({});
        expect(test.shard.queryCalls).toBe(1);
    });

    it("does not let a late permanent failure revoke a replacement registration", async () => {
        expect.hasAssertions();

        const test = setup();
        const entered = gate();
        const release = gate();

        test.authority.mockImplementationOnce(async () => {
            entered.resolve();
            await release.promise;
            throw new LunoraError("FORBIDDEN", "old request denied");
        });

        const old = test.send();
        await entered.promise;

        await test.send();

        release.resolve();
        await old;

        expect(test.socket.attachment.subs.view).toBeDefined();
        expect(test.socket.frames.filter((frame) => frame.type === "error")).toEqual([]);
        expect(test.shard.queryCalls).toBe(1);
    });

    it.each(["FORBIDDEN", "INTERNAL"])("does not mistake a refresh handler's %s for an authority refusal", async (code) => {
        expect.hasAssertions();

        const test = setup();

        await test.send();

        test.shard.queryFailure = new LunoraError(code, "handler failed");

        await test.write();

        expect(test.socket.attachment.subs.view).toBeDefined();
        expect(test.socket.frames.filter((frame) => frame.type === "error")).toEqual([]);
    });
});
