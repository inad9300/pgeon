// TODO Publish to DefinitelyTyped (https://github.com/zhm/node-pg-query-native).
// https://github.com/DefinitelyTyped/DefinitelyTyped/tree/master/types.
declare module 'pg-query-native' {
    export function parse(query: string): {
        query: any[]
        error: {
            message: string
            fileName: string
            lineNumber: number
            cursorPosition: number
        }
    }
}
