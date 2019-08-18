import {Client, Pool, QueryResult, QueryConfig} from 'pg'

// TODO Think about pg's custom parser role.
type Column = any // null | boolean | number | string | Date | Uint8Array

type Row = {[column: string]: Column}

interface $QueryResult<R extends Row> extends QueryResult {
    rows: R[]
}

// TODO Add 'types' to `pg.QueryConfig` (https://node-postgres.com/features/queries#Types).
type $QueryConfig = Omit<QueryConfig, 'text' | 'values'>

declare module 'pg' {
    export interface Client {
        $query<R extends Row>(queryParts: TemplateStringsArray, ...values: Column[]): Promise<$QueryResult<R>>
        $query<R extends Row>(queryConfig: $QueryConfig): ((queryParts: TemplateStringsArray, ...values: Column[]) => Promise<$QueryResult<R>>)
    }

    export interface Pool {
        $query<R extends Row>(queryParts: TemplateStringsArray, ...values: Column[]): Promise<$QueryResult<R>>
        $query<R extends Row>(queryConfig: $QueryConfig): ((queryParts: TemplateStringsArray, ...values: Column[]) => Promise<$QueryResult<R>>)
    }
}

const isArray: ((arg: any) => arg is TemplateStringsArray) = Array.isArray as any

function getQueryText(queryParts: TemplateStringsArray) {
    const last = queryParts.length - 1
    let text = ''
    for (let i = 0; i < last; ++i) {
        text += queryParts[i] + '$' + (i + 1)
    }
    text += queryParts[last]
    return text
}

/**
 * Template literal tag used to identify SQL queries in the code for static analysis.
 *
 * It being a template literal tag with placeholders assumed to be values guarantees that the
 * string is static, thus allowing to distinguish between static and dynamic queries. Additionally,
 * in-line arguments minimize indexing mistakes between the query placeholders and the arguments.
 */
function $query<R extends Row>(this: Client | Pool, queryParts: TemplateStringsArray, ...values: Column[]): Promise<$QueryResult<R>>
function $query<R extends Row>(this: Client | Pool, queryConfig: $QueryConfig): ((queryParts: TemplateStringsArray, ...values: Column[]) => Promise<$QueryResult<R>>)
function $query<R extends Row>(this: Client | Pool, queryPartsOrQueryConfig: TemplateStringsArray | $QueryConfig, ...valuesOrNothing: Column[]): Promise<$QueryResult<R>> | ((queryParts: TemplateStringsArray, ...values: Column[]) => Promise<$QueryResult<R>>) {
    if (isArray(queryPartsOrQueryConfig)) {
        return this.query(getQueryText(queryPartsOrQueryConfig), valuesOrNothing)
    } else {
        return (queryParts: TemplateStringsArray, ...values: Column[]): Promise<$QueryResult<R>> =>
            this.query({...queryPartsOrQueryConfig, text: getQueryText(queryParts), values})
    }
}

Client.prototype.$query = Pool.prototype.$query = $query
