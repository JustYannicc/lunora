import type { Node as AstNode } from "ts-morph";
import { Node, VariableDeclarationKind } from "ts-morph";

import { isServerSurfaceModule } from "../../../module-specifiers";
import { walkBuilderChain } from "../../builder-chain";

const FACTORIES = new Set(["query", "mutation", "action", "stream", "internalQuery", "internalMutation", "internalAction"]);

/** Resolve immutable aliases without trusting a type assertion or an identifier's spelling. */
const staticBuilderChain = (receiver: AstNode): { factory?: string; steps: ReturnType<typeof walkBuilderChain>["steps"] } => {
    const steps: ReturnType<typeof walkBuilderChain>["steps"] = [];
    const visited = new Set<AstNode>();
    let current: AstNode | undefined = receiver;

    for (let depth = 0; current !== undefined && depth < 64; depth += 1) {
        if (visited.has(current)) {
            break;
        }

        visited.add(current);
        const walked = walkBuilderChain(current);
        steps.push(...walked.steps);
        const root = walked.root;

        if (!root || !Node.isIdentifier(root)) {
            break;
        }

        const symbol = root.getSymbol();

        for (const declaration of symbol?.getDeclarations() ?? []) {
            if (Node.isImportSpecifier(declaration) && isServerSurfaceModule(declaration.getImportDeclaration().getModuleSpecifierValue())) {
                const factory = declaration.getName();

                return { ...(FACTORIES.has(factory) ? { factory } : {}), steps };
            }
        }

        const declaration = (symbol?.isAlias() ? symbol.getAliasedSymbol() : symbol)?.getValueDeclaration();

        if (
            !declaration ||
            !Node.isVariableDeclaration(declaration) ||
            declaration.getVariableStatement()?.getDeclarationKind() !== VariableDeclarationKind.Const
        ) {
            break;
        }

        current = declaration.getInitializer();
    }

    return { steps };
};

export { staticBuilderChain };
