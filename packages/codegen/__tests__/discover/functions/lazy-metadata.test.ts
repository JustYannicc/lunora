import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ModuleKind, ModuleResolutionKind, Project, SyntaxKind } from "ts-morph";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import discoverFunctions from "../../../src/discover/functions";
import { lazyMetadataFromBuilderChain } from "../../../src/discover/functions/internal/lazy-metadata";
import emitFunctions from "../../../src/emit/functions";

let directory: string;
let project: Project;

beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "lunora-lazy-metadata-"));
    project = new Project({
        compilerOptions: { module: ModuleKind.ESNext, moduleResolution: ModuleResolutionKind.Bundler },
        skipAddingFilesFromTsConfig: true,
    });
});

afterEach(() => {
    rmSync(directory, { force: true, recursive: true });
});

const write = (name: string, source: string): void => {
    writeFileSync(join(directory, name), source);
};
const discover = () => discoverFunctions(project, directory, { lazyFunctions: true });

describe("lazy metadata discovery", () => {
    it("preserves inherited prices and credential headers through local and imported aliases", () => {
        expect.assertions(5);
        write(
            "base.ts",
            `import { query as q } from "@lunora/server";
            const paid = q.x402({ price: "$0.10" });
            export const base = paid.expose({ rest: true, cache: { scope: "private", maxAge: 60, credentialHeaders: ["x-api-key", "x-tenant"], vary: "accept", tag: "posts", staleWhileRevalidate: 10 } });`,
        );
        write(
            "posts.ts",
            `import { base as shared } from "./base";
            const local = shared; const again = local;
            export const premium = again.query(() => "paid");`,
        );
        const functions = discover();
        const premium = functions.find((entry) => entry.exportName === "premium");
        expect(premium?.x402).toEqual({ price: "$0.10" });
        expect(premium?.expose).toEqual({
            rest: true,
            cache: { scope: "private", maxAge: 60, credentialHeaders: ["x-api-key", "x-tenant"], vary: "accept", tag: "posts", staleWhileRevalidate: 10 },
        });
        const generated = emitFunctions({ functions, lazyFunctions: true });
        expect(generated).toContain('"x402":{"price":"$0.10"}');
        expect(generated).toContain('"credentialHeaders":["x-api-key","x-tenant"]');
        expect(functions).toHaveLength(1);
    });

    it("preserves public credential-cache exclusions and the last authored metadata", () => {
        expect.assertions(2);
        write(
            "posts.ts",
            `import { query } from "@lunora/server";
            const base = query.x402({ price: "$1" }).expose({ rest: false });
            export const premium = base.x402({ price: "$2" }).expose({ rest: true, cache: { maxAge: 60, scope: "public", credentialHeaders: ["x-api-key"] } }).query(() => 1);`,
        );
        const [premium] = discover();
        expect(premium?.x402).toEqual({ price: "$2" });
        expect(premium?.expose).toEqual({ rest: true, cache: { maxAge: 60, scope: "public", credentialHeaders: ["x-api-key"] } });
    });

    it.each([
        "expose(config)",
        "expose({ rest: enabled })",
        "expose({ rest: true, extra: true })",
        "expose({ ...config, rest: true })",
        "expose({ rest: true, cache: policy })",
        'expose({ rest: true, cache: { scope: "public", maxAge: ttl } })',
        'expose({ rest: true, cache: { scope: "public", maxAge: 60, credentialHeaders: headers } })',
        'expose({ rest: true, cache: { scope: "public", maxAge: 60, credentialHeaders: [header] } })',
        'expose({ rest: true, cache: { scope: "public", maxAge: 60, vary: vary } })',
        'expose({ rest: true, cache: { scope: "public", maxAge: 60, tag: tag } })',
        'expose({ rest: true, cache: { scope: "public", maxAge: 60, futureField: true } })',
        "expose({ rest: true, cache: { maxAge: 60 } })",
        'expose({ rest: true, cache: { scope: "public" } })',
        "expose({ rest: true, rest: false })",
        "x402({ price: getPrice() })",
        'x402({ price: "$1", ...config })',
        'x402({ price: "$1", futureField: true })',
    ])("rejects unreadable or unsupported lazy metadata: %s", (modifier) => {
        expect.assertions(2);
        write(
            "posts.ts",
            `import { query } from "@lunora/server";
            const base = query.${modifier}; export const premium = base.query(() => 1);`,
        );
        expect(() => discover()).toThrow(/lazy function registry needs literal/u);
        expect(() => discoverFunctions(project, directory)).not.toThrow();
    });

    it.each([
        'declare const opaque: { __lunoraProcedure: "query"; query: (h: unknown) => unknown }; export const premium = opaque.query(() => 1);',
        'const a = b; const b = a; export const premium = (a as { __lunoraProcedure: "query"; query: (h: unknown) => unknown }).query(() => 1);',
        "let base = query; export const premium = base.query(() => 1);",
    ])("fails closed for unresolved or mutable builder roots", (body) => {
        expect.assertions(1);
        write("posts.ts", `import { query } from "@lunora/server"; ${body}`);
        expect(() => discover()).toThrow(/resolvable Lunora factory root/u);
    });

    it("does not trust an unimported factory name", () => {
        expect.assertions(1);
        const source = project.createSourceFile(join(directory, "unknown.ts"), "query.expose({ rest: true }).query(() => 1)");
        const terminal = source.getDescendantsOfKind(SyntaxKind.CallExpression)[0]!;
        const callee = terminal.getExpressionIfKindOrThrow(SyntaxKind.PropertyAccessExpression);
        expect(() => lazyMetadataFromBuilderChain(callee.getExpression())).toThrow(/resolvable Lunora factory root/u);
    });

    it("keeps internal visibility across imported base aliases", () => {
        expect.assertions(1);
        write("base.ts", `import { internalQuery as q } from "@lunora/server"; export const base = q.input({});`);
        write("posts.ts", `import { base } from "./base"; export const secret = base.query(() => 1);`);
        expect(discover()[0]?.visibility).toBe("internal");
    });
});
