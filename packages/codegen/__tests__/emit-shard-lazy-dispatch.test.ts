import { Project } from "ts-morph";
import { describe, expect, it } from "vitest";

import discoverSchema from "../src/discover/schema";
import { emitShard } from "../src/emit";

const SCHEMA = `
    import { defineSchema, defineTable, v } from "@lunora/server";

    export const schema = defineSchema({
        messages: defineTable({ text: v.string() }),
    });
`;

const emit = (lazyFunctions: boolean): string => {
    const project = new Project({ skipAddingFilesFromTsConfig: true, useInMemoryFileSystem: true });
    const schemaPath = "/virtual/lunora/schema.ts";

    project.createSourceFile(schemaPath, SCHEMA);

    return emitShard({ lazyFunctions, schema: discoverSchema(project, schemaPath) });
};

describe("emitted lazy function dispatch", () => {
    it("keeps eager output free of lazy loading behavior", () => {
        expect.assertions(2);

        const shard = emit(false);

        expect(shard).not.toContain("getLoadedFunction");
        expect(shard).not.toContain("prepareFunctionInRequestScope");
    });

    it("keeps preparation synchronous after module resolution and pins request-scoped entry points", () => {
        expect.assertions(12);

        const shard = emit(true);

        expect(shard).toContain("import { LUNORA_FUNCTIONS, LUNORA_LIFECYCLE_HOOKS, LUNORA_MIGRATIONS, getLoadedFunction, loadFunction }");
        expect(shard).toContain("protected override prepareFunction(functionPath: string): Promise<void> | undefined {");
        expect(shard).toContain("if (getLoadedFunction(functionPath) !== undefined) {");
        expect(shard).toContain("return loadFunction(functionPath).then(() => undefined);");
        expect(shard).toContain("const loaded = getLoadedFunction(functionPath) ?? await loadFunction(functionPath);");
        expect(shard).toContain("const requestScope = this.captureRequestScope();");
        expect(shard).toContain("const preparation = this.prepareFunction(functionPath);");
        expect(shard).toContain("await preparation;");
        expect(shard).toContain("this.restoreRequestScope(requestScope);");
        expect(shard.match(/const requestScope = this\.captureRequestScope\(\);/gu)).toHaveLength(2);
        expect(shard).not.toContain("prepareFunctionInRequestScope");
        expect(shard).not.toContain("protected override async prepareFunction(");
    });
});
