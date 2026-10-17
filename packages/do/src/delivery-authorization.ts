import { LunoraError } from "@lunora/errors";
import type { SqlExec, SubscriptionIdentity } from "@lunora/shard-engine";
import { RELAY_SIGNATURE_HEADER, siblingSecretOf, signSiblingBody } from "@lunora/shard-engine";

/** Opaque authority scope and its monotonic revision. No access policy is stored on the shard. */
export interface DeliveryPermit {
    /** Opaque authority key, at most 256 UTF-8 bytes to bound persisted socket metadata. */
    readonly scope: string;
    readonly epoch: number;
}

export interface DeliveryAuthorizationRequest {
    readonly identity: SubscriptionIdentity;
    readonly shardKey: string | undefined;
    readonly functionPath: string;
    readonly args: Record<string, unknown>;
    readonly transport: "subscription" | "rpc";
}

/**
 * Resolve against current authority, including before receipt/cache replay.
 * The application commits policy and pending invalidation together, denies new
 * permits immediately, and retries installDeliveryFence before acknowledging
 * revocation. It owns affected scopes; the shard stores no grant or topology copy.
 */
export type DeliveryAuthorization = (request: DeliveryAuthorizationRequest) => Promise<DeliveryPermit | undefined>;

export const validDeliveryPermit = (value: unknown): value is DeliveryPermit => {
    if (typeof value !== "object" || value === null) {
        return false;
    }

    const permit = value as Partial<DeliveryPermit>;

    return (
        typeof permit.scope === "string" &&
        permit.scope.length > 0 &&
        new TextEncoder().encode(permit.scope).byteLength <= 256 &&
        Number.isSafeInteger(permit.epoch) &&
        (permit.epoch ?? -1) >= 0
    );
};

class DeliveryAccessDenied extends LunoraError {
    public constructor() {
        super("FORBIDDEN", "AccessDenied");
    }
}

export const deliveryAccessDenied = (): LunoraError => new DeliveryAccessDenied();
export const isDeliveryAccessDenied = (error: unknown): boolean => error instanceof DeliveryAccessDenied;

/** SQL uses the host's synchronous storage and output durability gate, including after hibernation. */
export class DeliveryFence {
    private initialized = false;

    public constructor(private readonly sql: () => SqlExec) {}

    private storage(): SqlExec {
        const sql = this.sql();

        if (!this.initialized) {
            sql.exec("CREATE TABLE IF NOT EXISTS __lunora_delivery_fence (scope TEXT PRIMARY KEY, epoch INTEGER NOT NULL)");
            this.initialized = true;
        }

        return sql;
    }

    public install(permit: DeliveryPermit): void {
        if (!validDeliveryPermit(permit)) {
            throw new TypeError("invalid delivery fence");
        }

        this.storage().exec(
            "INSERT INTO __lunora_delivery_fence(scope, epoch) VALUES (?, ?) ON CONFLICT(scope) DO UPDATE SET epoch = MAX(epoch, excluded.epoch)",
            permit.scope,
            permit.epoch,
        );
    }

    public allows(permit: DeliveryPermit | undefined): boolean {
        if (!validDeliveryPermit(permit)) {
            return false;
        }

        const row = this.storage().exec<{ epoch: number }>("SELECT epoch FROM __lunora_delivery_fence WHERE scope = ?", permit.scope).toArray()[0];

        return row === undefined || permit.epoch >= row.epoch;
    }
}

/** Install through a private namespace binding. Acknowledgement covers this shard's durable local fence. */
export const installDeliveryFence = async (
    target: { fetch: (url: string, init?: RequestInit) => Promise<Response> },
    options: { env: unknown; shardKey: string | undefined; fence: DeliveryPermit },
): Promise<void> => {
    const secret = siblingSecretOf(options.env);
    if (secret === undefined || !validDeliveryPermit(options.fence)) {
        throw new TypeError("delivery fence requires a valid marker and LUNORA_RELAY_SECRET");
    }
    const body = JSON.stringify({ type: "delivery-fence", shardKey: options.shardKey, fence: options.fence });
    const response = await target.fetch("https://shard.internal/_lunora/delivery-fence", {
        method: "POST",
        body,
        headers: { [RELAY_SIGNATURE_HEADER]: await signSiblingBody(secret, body) },
    });
    if (response.status !== 204) {
        throw new Error(`delivery fence was not acknowledged (${String(response.status)})`);
    }
};
