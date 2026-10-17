import type { ShardDOState, SubscriptionOutcome } from "@lunora/do";
import { ShardDO } from "@lunora/do";
import type { SqlExec } from "@lunora/shard-engine";
import { runShardMigrations } from "@lunora/shard-engine";
import { DurableObject } from "cloudflare:workers";

export const DELIVERY_TEST_SECRET = "delivery-authorization-workerd-fixture";

interface Policy {
    allowed: boolean;
    epoch: number;
}

// Import the package export so this regression runs the built ShardDO shipped to users.
class AuthorizedShard extends ShardDO {
    public attempts = 0;
    public parkActions = false;
    private readonly actions = new Map<
        number,
        { entered: ReturnType<typeof Promise.withResolvers<void>>; released: ReturnType<typeof Promise.withResolvers<void>> }
    >();

    public action(epoch: number) {
        let action = this.actions.get(epoch);
        if (!action) {
            action = { entered: Promise.withResolvers<void>(), released: Promise.withResolvers<void>() };
            this.actions.set(epoch, action);
        }
        return action;
    }

    public constructor(
        state: ShardDOState,
        private readonly policy: Policy,
    ) {
        super(
            state,
            { LUNORA_RELAY_SECRET: DELIVERY_TEST_SECRET },
            {
                deliveryAuthorization: ({ identity }) =>
                    Promise.resolve(identity.userId === "alice" && policy.allowed ? { scope: "fixture-scope", epoch: policy.epoch } : undefined),
            },
        );
    }

    // eslint-disable-next-line class-methods-use-this -- mirrors generated mutation classification
    protected override isMutationFunction(functionPath: string): boolean {
        return functionPath === "fixture:write";
    }

    public override handleRpc(functionPath: string, args: Record<string, unknown>): Promise<unknown> {
        if (functionPath === "fixture:action") {
            this.attempts += 1;
            const epoch = this.policy.epoch;
            const action = this.action(epoch);
            action.entered.resolve();
            return this.parkActions ? action.released.promise.then(() => ({ epoch })) : Promise.resolve({ epoch });
        }
        return this.runInTransaction(() => {
            this.attempts += 1;
            const sql = this.sql as SqlExec;
            sql.exec("UPDATE delivery_fixture SET runs = runs + 1");
            const result = { runs: sql.exec<{ runs: number }>("SELECT runs FROM delivery_fixture").toArray()[0]?.runs };
            this.commitMutationBookkeeping(result);
            if (args.fail) {
                throw new Error("fixture rollback after receipt");
            }
            return result;
        });
    }

    // eslint-disable-next-line class-methods-use-this -- supplies an ordinary live-query result through the production sender
    protected override executeSubscription(): Promise<SubscriptionOutcome> {
        return Promise.resolve({ result: { value: "private" }, tables: new Set(["delivery_fixture"]) });
    }
}

export class DeliveryAuthorizationTestDO extends DurableObject {
    private readonly boot = crypto.randomUUID();
    private readonly policy: Policy = { allowed: true, epoch: 0 };
    private readonly shard: AuthorizedShard;

    public constructor(context: DurableObjectState, env: Cloudflare.Env) {
        super(context, env);
        runShardMigrations(context.storage.sql as unknown as SqlExec, { tables: {} });
        context.storage.sql.exec("CREATE TABLE IF NOT EXISTS delivery_fixture(runs INTEGER NOT NULL)");
        context.storage.sql.exec("INSERT INTO delivery_fixture SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM delivery_fixture)");
        this.shard = new AuthorizedShard(context as unknown as ShardDOState, this.policy);
    }

    public setPolicy(epoch: number, allowed = true): void {
        this.policy.epoch = epoch;
        this.policy.allowed = allowed;
    }

    public rejectEpochZeroMarker(): void {
        this.ctx.storage.sql.exec(
            "CREATE TRIGGER reject_old_marker BEFORE INSERT ON __lunora_delivery_receipt WHEN NEW.epoch = 0 BEGIN SELECT RAISE(FAIL, 'marker unavailable'); END",
        );
    }

    public rejectEpochOneRow(): void {
        this.ctx.storage.sql.exec(
            `CREATE TRIGGER reject_new_row BEFORE INSERT ON __idempotency WHEN NEW.result_json = '{"epoch":1}' BEGIN SELECT RAISE(FAIL, 'row unavailable'); END`,
        );
    }

    public parkActions(): void {
        this.shard.parkActions = true;
    }

    public waitForAction(epoch: number): Promise<void> {
        return this.shard.action(epoch).entered.promise;
    }

    public releaseAction(epoch: number): void {
        this.shard.action(epoch).released.resolve();
    }

    public snapshot() {
        const sql = this.ctx.storage.sql;
        return {
            attempts: this.shard.attempts,
            boot: this.boot,
            runs: sql.exec<{ runs: number }>("SELECT runs FROM delivery_fixture").toArray()[0]?.runs,
            receipts: sql
                .exec<{ identity: string; mutation_id: string; result_json: string }>("SELECT identity, mutation_id, result_json FROM __idempotency")
                .toArray(),
            permits: sql
                .exec<{ identity: string; mutation_id: string; scope: string; epoch: number }>(
                    "SELECT identity, mutation_id, scope, epoch FROM __lunora_delivery_receipt",
                )
                .toArray(),
            fences: sql.exec<{ scope: string; epoch: number }>("SELECT scope, epoch FROM __lunora_delivery_fence").toArray(),
            attachments: this.ctx.getWebSockets().map((socket) => {
                const attachment = socket.deserializeAttachment() as {
                    subs: Record<string, unknown>;
                    deliveryPermits: Record<string, { scope: string; epoch: number }>;
                };
                return { subscriptionIds: Object.keys(attachment.subs), deliveryPermits: attachment.deliveryPermits };
            }),
        };
    }

    public override fetch(request: Request): Promise<Response> {
        return this.shard.fetch(request);
    }

    public override webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
        return this.shard.webSocketMessage(socket, message);
    }

    public override webSocketClose(socket: WebSocket, code: number, reason: string, clean: boolean): Promise<void> {
        return this.shard.webSocketClose(socket, code, reason, clean);
    }

    public override webSocketError(socket: WebSocket, error: unknown): Promise<void> {
        return this.shard.webSocketError(socket, error);
    }
}
