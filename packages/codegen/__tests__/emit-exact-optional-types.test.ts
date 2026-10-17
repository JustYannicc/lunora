import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { ModuleKind, ModuleResolutionKind, Project, ScriptTarget } from "ts-morph";
import { describe, expect, it } from "vitest";

import { runCodegen } from "../src/index";

const here = dirname(fileURLToPath(import.meta.url));

describe("generated strict optional properties", () => {
    it("compiles the complete shard for a counter app with identity", () => {
        expect.assertions(1);

        const workdir = mkdtempSync(join(here, "fixtures", ".workdir-"));
        const app = join(workdir, "lunora");

        try {
            mkdirSync(app);
            writeFileSync(
                join(app, "schema.ts"),
                `
                import { defineSchema, defineTable, v } from "@lunora/server";
                export default defineSchema({ counter: defineTable({ value: v.number() }) });
            `,
            );
            writeFileSync(
                join(app, "identity.ts"),
                `
                import { defineIdentity, v } from "@lunora/server";
                export const identity = defineIdentity({ userId: v.string() });
            `,
            );
            writeFileSync(
                join(app, "counter.ts"),
                `
                import { mutation, query, v } from "./_generated/server.js";
                export const read = query.input({}).query(async () => 0);
                export const write = mutation.input({ value: v.number() }).mutation(async ({ args }) => args.value);
            `,
            );
            runCodegen({ lint: false, projectRoot: workdir, wranglerQueueProducers: [] });

            const project = new Project({
                compilerOptions: {
                    exactOptionalPropertyTypes: true,
                    module: ModuleKind.ESNext,
                    moduleResolution: ModuleResolutionKind.Bundler,
                    noEmit: true,
                    skipLibCheck: true,
                    strict: true,
                    target: ScriptTarget.ES2022,
                },
            });

            project.addSourceFileAtPath(join(app, "_generated", "shard.ts"));
            expect(project.formatDiagnosticsWithColorAndContext(project.getPreEmitDiagnostics())).toBe("");
        } finally {
            rmSync(workdir, { force: true, recursive: true });
        }
    });
});
