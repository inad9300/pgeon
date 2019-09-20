import {Client, Pool, QueryConfig, QueryResult, QueryResultRow} from 'pg'

// This is required because of the weird way in which `pg.Pool` is initialized
// (see https://github.com/brianc/node-postgres/blob/v7.12.1/lib/index.js#L31).
import * as PgPool from 'pg-pool'

type $QueryConfig = Omit<QueryConfig, 'text' | 'values'>

declare module 'pg' {
    export interface Client {
        $query<R extends QueryResultRow>(queryParts: TemplateStringsArray, ...values: any[]): Promise<QueryResult<R>>
        $query<R extends QueryResultRow>(queryConfig: $QueryConfig): ((queryParts: TemplateStringsArray, ...values: any[]) => Promise<QueryResult<R>>)
    }

    export interface Pool {
        $query<R extends QueryResultRow>(queryParts: TemplateStringsArray, ...values: any[]): Promise<QueryResult<R>>
        $query<R extends QueryResultRow>(queryConfig: $QueryConfig): ((queryParts: TemplateStringsArray, ...values: any[]) => Promise<QueryResult<R>>)
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
function $query<R extends QueryResultRow>(this: Client | Pool, queryParts: TemplateStringsArray, ...values: any[]): Promise<QueryResult<R>>
function $query<R extends QueryResultRow>(this: Client | Pool, queryConfig: $QueryConfig): ((queryParts: TemplateStringsArray, ...values: any[]) => Promise<QueryResult<R>>)
function $query<R extends QueryResultRow>(this: Client | Pool, queryPartsOrQueryConfig: TemplateStringsArray | $QueryConfig, ...valuesOrNothing: any[]): Promise<QueryResult<R>> | ((queryParts: TemplateStringsArray, ...values: any[]) => Promise<QueryResult<R>>) {
    if (isArray(queryPartsOrQueryConfig)) {
        return this.query(getQueryText(queryPartsOrQueryConfig), valuesOrNothing)
    } else {
        return (queryParts: TemplateStringsArray, ...values: any[]): Promise<QueryResult<R>> =>
            this.query({...queryPartsOrQueryConfig, text: getQueryText(queryParts), values})
    }
}

Client.prototype.$query = PgPool.prototype.$query = $query
