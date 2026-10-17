import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build, transform } from "esbuild";
import { describe, expect, it } from "vitest";

import { emitFunctions, emitLazyFunctionModules } from "../src/emit";
import type { FunctionIR } from "../src/ir";

const makeFunction = (exportName: string, overrides: Partial<FunctionIR> = {}): FunctionIR => ({
    args: { id: { kind: "string" } },
    exportName,
    filePath: "posts",
    kind: "query",
    returnType: "string",
    ...overrides,
});

describe("lazy function registry", () => {
    it("emits static paid and REST metadata while deferring the user module and AOT validators", () => {
        expect.assertions(8);

        const definition = makeFunction("premium", {
            expose: { cache: { maxAge: 60, scope: "private" }, rest: true },
            x402: { price: "$0.05" },
        });
        const options = { functions: [definition], lazyFunctions: true };
        const emitted = emitFunctions(options);
        const modules = emitLazyFunctionModules(options);
        const module = modules["fn/lunora_posts_0.ts"] ?? "";

        expect(emitted).not.toContain('import * as lunora_posts_0 from "../posts.js";');
        expect(emitted).toContain('"posts:premium": { args: {}, handler: unloadedFunctionHandler');
        expect(emitted).toContain(
            '"posts:premium": {"argumentDescriptors":[{"kind":"string","name":"id","optional":false}],"exportName":"premium","kind":"query","moduleId":"lunora_posts_0","expose":{"cache":{"maxAge":60,"scope":"private"},"rest":true},"x402":{"price":"$0.05"}}',
        );
        expect(emitted).toContain('"lunora_posts_0": () => import("./fn/lunora_posts_0.js")');
        expect(emitted).toContain("const loaded = module[manifest.exportName];");
        expect(emitted).not.toContain("installCompiledValidatorMap");
        expect(module).toContain('import * as lunora_posts_0 from "../../posts.js";');
        expect(module).toContain("installCompiledValidatorMap(lunora_posts_0.premium.args");
    });

    it("deduplicates handlers from the same module and emits room routes without loading handlers", () => {
        expect.assertions(4);

        const first = makeFunction("one");
        const second = makeFunction("two", { args: {}, returnType: "void" });
        const room = makeFunction("chat", { args: {}, channel: { kind: "room", name: "chat" } });
        const options = { functions: [first, second, room], lazyFunctions: true };
        const emitted = emitFunctions(options);
        const modules = emitLazyFunctionModules(options);

        expect(Object.keys(modules)).toEqual(["fn/lunora_posts_0.ts"]);
        expect(emitted.match(/\(\) => import\("\.\/fn\/lunora_posts_0\.js"\)/gu)).toHaveLength(1);
        expect(emitted).toContain('topic: "room:chat"');
        expect(emitted).not.toContain("for (const [path, fn] of Object.entries(LUNORA_FUNCTIONS))");
    });

    it("fails closed when a lazy paid procedure has a dynamic price", () => {
        expect.assertions(1);

        expect(() => emitFunctions({ functions: [makeFunction("premium", { x402: {} })], lazyFunctions: true })).toThrow(/literal `.x402\(\{ price \}\)`/u);
    });

    it("keeps eager output unchanged unless lazy mode is selected", () => {
        expect.assertions(2);

        const emitted = emitFunctions({ functions: [makeFunction("list")] });

        expect(emitted).toContain('import * as lunora_posts_0 from "../posts.js";');
        expect(emitted).not.toContain("LUNORA_FUNCTION_MANIFEST");
    });

    it("loads real handlers once, installs compiled validators, and retries a failed module import", async () => {
        expect.assertions(11);

        const projectRoot = mkdtempSync(join(tmpdir(), "lunora-lazy-registry-"));
        const generatedDirectory = join(projectRoot, "_generated");
        const shimDirectory = join(generatedDirectory, "fn");
        const packageDirectory = join(projectRoot, "node_modules", "@lunora");
        const stateName = `__lunoraLazyRegistry${Date.now()}${Math.random().toString(16).slice(2)}`;
        const definition = makeFunction("premium", { args: { name: { kind: "string" } } });
        const options = { functions: [definition, makeFunction("sibling", { args: {} })], lazyFunctions: true };
        const registrySource = emitFunctions(options);
        const shimSource = emitLazyFunctionModules(options)["fn/lunora_posts_0.ts"];

        try {
            mkdirSync(shimDirectory, { recursive: true });
            mkdirSync(join(packageDirectory, "server"), { recursive: true });
            mkdirSync(join(packageDirectory, "errors"), { recursive: true });
            mkdirSync(join(packageDirectory, "values"), { recursive: true });
            writeFileSync(join(projectRoot, "package.json"), JSON.stringify({ type: "module" }));
            writeFileSync(join(packageDirectory, "server", "package.json"), JSON.stringify({ exports: "./index.js", type: "module" }));
            writeFileSync(join(packageDirectory, "server", "index.js"), "export class LunoraError extends Error {}\n");
            writeFileSync(join(packageDirectory, "errors", "package.json"), JSON.stringify({ exports: "./index.js", type: "module" }));
            writeFileSync(join(packageDirectory, "errors", "index.js"), "export class LunoraError extends Error {}\n");
            writeFileSync(join(packageDirectory, "values", "package.json"), JSON.stringify({ exports: "./index.js", type: "module" }));
            await build({
                bundle: true,
                entryPoints: [new URL("../../values/src/validator-map.ts", import.meta.url).pathname],
                external: ["@lunora/errors"],
                format: "esm",
                outfile: join(packageDirectory, "values", "index.js"),
                platform: "node",
            });
            writeFileSync(
                join(projectRoot, "posts.js"),
                `const state = globalThis[${JSON.stringify(stateName)}];
state.evaluations += 1;
state.onStart();
await new Promise((resolve) => { state.release = resolve; });
export const premium = {
  args: { name: { kind: "string", parse(value) { state.parseCalls += 1; return value; } } },
  kind: "query",
  handler(_context, args) { state.handlerCalls += 1; return args.name; }
};
export const sibling = { args: {}, kind: "query", handler() { return "sibling"; } };\n`,
            );

            const registryFile = join(generatedDirectory, "functions.mjs");
            const shimFile = join(shimDirectory, "lunora_posts_0.js");
            const shimPayloadFile = join(projectRoot, "lazy-shim.js");
            writeFileSync(registryFile, (await transform(registrySource, { format: "esm", loader: "ts", target: "node22" })).code);
            writeFileSync(shimPayloadFile, (await transform(shimSource, { format: "esm", loader: "ts", target: "node22" })).code);

            const runnerFile = join(projectRoot, "run-lazy-registry.mjs");
            writeFileSync(
                runnerFile,
                `import { copyFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const [configText] = process.argv.slice(2);
const config = JSON.parse(configText);
const state = { evaluations: 0, handlerCalls: 0, parseCalls: 0, release: undefined, onStart: undefined };
let signalStart;
const moduleStarted = new Promise((resolve) => { signalStart = resolve; });
state.onStart = signalStart;
globalThis[config.stateName] = state;

const registry = await import(pathToFileURL(config.registryFile));
const coldEntry = registry.getLoadedFunction("posts:premium");
const failed = await Promise.allSettled([registry.loadFunction("posts:premium"), registry.loadFunction("posts:premium")]);
copyFileSync(config.shimPayloadFile, config.shimFile);
const first = registry.loadFunction("posts:premium");
const second = registry.loadFunction("posts:premium");
await moduleStarted;
const coalescedEvaluations = state.evaluations;
state.release();
await Promise.all([first, second]);
const loaded = registry.LUNORA_FUNCTIONS["posts:premium"];
const values = await import(pathToFileURL(config.valuesFile));
const parsed = values.parseValidatorMap(loaded.args, { extra: true, name: "Ada" }, "args");
const handlerResult = await loaded.handler({}, { name: "Ada" });
const cachedEntry = await registry.loadFunction("posts:premium");
const synchronousEntry = registry.getLoadedFunction("posts:premium");
const sibling = registry.getLoadedFunction("posts:sibling");

process.stdout.write(JSON.stringify({
  cacheReusedEntry: cachedEntry === loaded,
  coldEntryMissing: coldEntry === undefined,
  synchronousEntry: synchronousEntry === loaded,
  siblingResult: sibling.handler(),
  coalescedEvaluations,
  finalEvaluations: state.evaluations,
  failedLoadsRejected: failed.every((result) => result.status === "rejected"),
  handlerCalls: state.handlerCalls,
  handlerResult,
  parseCalls: state.parseCalls,
  parsed,
}));
delete globalThis[config.stateName];
`,
            );
            const output = execFileSync(
                process.execPath,
                [
                    runnerFile,
                    JSON.stringify({
                        registryFile,
                        shimFile,
                        shimPayloadFile,
                        stateName,
                        valuesFile: join(packageDirectory, "values", "index.js"),
                    }),
                ],
                { encoding: "utf8" },
            );
            const result = JSON.parse(output) as {
                cacheReusedEntry: boolean;
                coldEntryMissing: boolean;
                synchronousEntry: boolean;
                siblingResult: string;
                coalescedEvaluations: number;
                failedLoadsRejected: boolean;
                finalEvaluations: number;
                handlerCalls: number;
                handlerResult: string;
                parseCalls: number;
                parsed: Record<string, unknown>;
            };

            expect(result.failedLoadsRejected).toBe(true);
            expect(result.coalescedEvaluations).toBe(1);
            expect(result.finalEvaluations).toBe(1);
            expect(result.cacheReusedEntry).toBe(true);
            expect(result.coldEntryMissing).toBe(true);
            expect(result.synchronousEntry).toBe(true);
            expect(result.siblingResult).toBe("sibling");
            expect(result.handlerCalls).toBe(1);
            expect(result.handlerResult).toBe("Ada");
            expect(result.parsed).toEqual({ name: "Ada" });
            expect(result.parseCalls).toBe(0);
        } finally {
            delete (globalThis as Record<string, unknown>)[stateName];
            rmSync(projectRoot, { force: true, recursive: true });
        }
    });
});
