import { encodeWire } from "@lunora/wire";
import { describe, expect, it } from "vitest";

import { installDeliveryFence } from "../src/delivery-authorization";
import { ROOT_SHARD_NAME } from "../src/shard-do";
import { gate, permit, secret, setup } from "./_helpers/delivery-authorization";

const batch = (paths: string[]) =>
    new Request("https://shard.internal/rpc-batch", {
        method: "POST",
        headers: { "x-lunora-userid": "alice" },
        body: JSON.stringify({ calls: paths.map((functionPath, id) => ({ id, functionPath, args: {}, mutationId: `operation-${String(id)}` })) }),
    });

describe("delivery-authorized transports", () => {
    it("dispatches and fences each batch slot without rejecting the whole batch", async () => {
        const test = setup();
        test.authority.mockImplementation(async ({ functionPath }) => (functionPath === "messages:denied" ? undefined : permit(0)));
        const response = await test.shard.fetch(batch(["messages:write", "messages:denied"]));
        expect(response.status).toBe(200);
        expect(await response.json()).toMatchObject({
            results: [
                { id: 0, status: 200, body: { result: { result: "receipt-payload" } } },
                { id: 1, status: 403, body: { error: { code: "FORBIDDEN" } } },
            ],
        });
        expect(test.shard.rpcRuns).toBe(1);
        expect(test.authority).toHaveBeenCalledTimes(2);
    });

    it("rechecks an earlier batch slot after a later entry awaits", async () => {
        const test = setup();
        const release = gate();
        test.shard.rpcGate = release.promise;
        test.authority.mockImplementation(async ({ functionPath }) => (functionPath === "messages:slow" ? { scope: "other", epoch: 0 } : permit(0)));
        const pending = test.shard.fetch(batch(["messages:write", "messages:slow"]));
        await test.shard.rpcStarted.promise;
        await test.fence();
        release.resolve();
        const response = await pending;
        expect(await response.json()).toMatchObject({
            results: [
                { id: 0, status: 409, body: { error: { code: "DELIVERY_FENCED" } } },
                { id: 1, status: 200, body: { result: { result: "receipt-payload" } } },
            ],
        });
    });

    it("passes decoded wire arguments to both subscription and RPC authorization", async () => {
        const test = setup();
        const args = { count: 9n, bytes: new Uint8Array([1, 2]).buffer };
        const encoded = encodeWire(args);
        await test.shard.fetch(
            new Request("https://shard.internal/rpc", {
                method: "POST",
                body: JSON.stringify({ functionPath: "messages:list", args: encoded }),
            }),
        );
        await test.shard.webSocketMessage(
            test.socket as unknown as WebSocket,
            JSON.stringify({
                type: "subscribe",
                id: "wire",
                query: { functionPath: "messages:list", args: encoded },
            }),
        );
        expect(test.authority.mock.calls.map(([request]) => request.args)).toEqual([args, args]);
    });

    it("reports an authority outage as retryable instead of access denial", async () => {
        const test = setup();
        test.authority.mockRejectedValue(new Error("authority unavailable"));
        const response = await test.write();
        expect(response.status).toBe(503);
        expect(await response.json()).toEqual({ error: { code: "SHARD_UNAVAILABLE", message: "delivery authority unavailable" } });
        expect(test.shard.rpcRuns).toBe(0);
    });

    it.each([ROOT_SHARD_NAME, "tenant:folder-1"])("binds named shard %s control requests to the exact host name", async (name) => {
        const test = setup(name);
        await test.send();
        expect(test.authority.mock.calls[0]?.[0].shardKey).toBe(name);
        await expect(
            installDeliveryFence(
                { fetch: (url, init) => test.shard.fetch(new Request(url, init)) },
                {
                    env: { LUNORA_RELAY_SECRET: secret },
                    shardKey: `${name}:other`,
                    fence: permit(1),
                },
            ),
        ).rejects.toThrow("400");
        expect((await test.fence()).status).toBe(204);
        expect(test.socket.attachment.subs).toEqual({});
    });
});
