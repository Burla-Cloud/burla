import { ParserConfig, JscTarget } from "@swc/core";
import { PluginOption } from "vite";
type Options = {
    /**
     * Control where the JSX factory is imported from.
     * @default "react"
     */
    jsxImportSource?: string;
    /**
     * Enable TypeScript decorators. Requires experimentalDecorators in tsconfig.
     * @default false
     */
    tsDecorators?: boolean;
    /**
     * Use SWC plugins. Enable SWC at build time.
     * @default undefined
     */
    plugins?: [string, Record<string, any>][];
    /**
     * Set the target for SWC in dev. This can avoid to down-transpile private class method for example.
     * For production target, see https://vitejs.dev/config/build-options.html#build-target
     * @default "es2020"
     */
    devTarget?: JscTarget;
    /**
     * Override the default include list (.ts, .tsx, .mts, .jsx, .mdx).
     * This requires to redefine the config for any file you want to be included.
     * If you want to trigger fast refresh on compiled JS, use `jsx: true`.
     * Exclusion of node_modules should be handled by the function if needed.
     */
    parserConfig?: (id: string) => ParserConfig | undefined;
};
declare const react: (_options?: Options) => PluginOption[];
export default react;
