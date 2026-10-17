import { isLazyFunctionsEnabled, LAZY_FUNCTIONS_ENV } from "@lunora/config";

/** Match Vite and Rspack: either opt-in enables lazy imports; false does not veto the environment. */
const resolveLazyFunctions = (flag?: boolean): boolean => flag === true || isLazyFunctionsEnabled(process.env[LAZY_FUNCTIONS_ENV]);

export default resolveLazyFunctions;
