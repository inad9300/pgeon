import { createConnection as createTcpConnection, Socket } from 'net'
import { connect as createTlsConnection, ConnectionOptions } from 'tls'
import {
   createBindMessage,
   createDescribeMessage,
   createParseMessage,
   createSimpleQueryMessage,
   createStartupMessage,
   DescribeType,
   executeUnnamedPortalMessage,
   md5,
   sslRequestMessage,
   syncMessage
} from './frontend'
import {
   BackendFailure,
   ColumnValue,
   handleQueryExecution,
   handleQueryPreparation,
   handleSimpleQueryExecution,
   handleStartupPhase,
   isSslSuppported,
   QueryMetadata,
   QueryResult,
   Row
} from './backend'

// IDEA
// class PgeonError<T extends string, D = void> extends Error {
//    readonly name = 'PgeonError'
//    readonly time = Date.now()
//
//    constructor(readonly type: T, readonly data: D) {
//       super()
//    }
// }

export interface PoolOptions {
   host: string
   port: number
   database: string
   username: string
   password: string
   ssl: ConnectionOptions
   minConnections: number
   maxConnections: number
   connectTimeout: number
   queryTimeout: number
   idleTimeout: number
}

export interface Client {
   run<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(query: Query<R, P>): CancellablePromise<QueryResult<R>>
}

export interface Pool extends Client {
   getQueryMetadata(query: string): Promise<QueryMetadata>
   transaction(callback: (client: Client) => Promise<void>): Promise<void>
   destroy(): void
}

export interface Query<_ROW extends Row = Row, _PARAMS extends ColumnValue[] = ColumnValue[]> {
   sql: string
   params?: ColumnValue[]
   id?: string
   metadata?: QueryMetadata
}

export interface CancellablePromise<T> extends Promise<T> {
   cancel(): void
}

export class QueryCancelledError extends Error {
   name = 'QueryCancelledError'
}

export function sql<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(sqlParts: TemplateStringsArray, ...params: P): Query<R, P> {
   const lastIdx = params.length
   const uniqueParams = [] as any[] as P
   const argIndices: number[] = []

   out:
   for (let i = 0; i < lastIdx; ++i) {
      const val = params[i]
      const argIdx = uniqueParams.length
      for (let j = 0; j < argIdx; ++j) {
         if (val === uniqueParams[j]) {
            argIndices.push(j + 1)
            continue out
         }
      }
      argIndices.push(argIdx + 1)
      uniqueParams.push(val)
   }

   let sql = ''
   for (let i = 0; i < lastIdx; ++i)
      sql += sqlParts[i] + '$' + argIndices[i]

   sql += sqlParts[lastIdx]

   return {
      sql,
      params: uniqueParams,
      id: md5(sql)
   }
}

export interface Connection extends Socket {
   cancelKey: Buffer
   preparedQueries: {
      [queryId: string]: QueryMetadata
   }
}

export function newPool(options: Partial<PoolOptions> = {}): Pool {
   const { env } = process

    // Environment variables read from the following sources:
    // 1. https://postgresql.org/docs/current/libpq-envars.html
    // 2. https://hub.docker.com/_/postgres
    options.host           = options.host           ||  env.PGHOST     || 'localhost'
    options.port           = options.port           || (env.PGPORT      ? parseInt(env.PGPORT, 10)  : 5432)
    options.database       = options.database       ||  env.PGDATABASE || env.POSTGRES_DB          || 'postgres'
    options.username       = options.username       ||  env.PGUSER     || env.POSTGRES_USER        || 'postgres'
    options.password       = options.password       ||  env.PGPASSWORD || env.POSTGRES_PASSWORD
    options.minConnections = options.minConnections || 2
    options.maxConnections = options.maxConnections || 8
    options.connectTimeout = options.connectTimeout || 15_000
    options.queryTimeout   = options.queryTimeout   || 120_000
    options.idleTimeout    = options.idleTimeout    || 300_000

   let openingConnections = 0
   const openConnections: Connection[] = []
   const availableConnections: Connection[] = []
   const waitingForConnection: ((conn: Connection) => void)[] = []

   for (let i = 0; i < options.minConnections; ++i)
      tryOpenConnection()

   function tryOpenConnection(retryDelay = 16) {
      openingConnections++
      openConnection(options as PoolOptions)
         .then(conn => {
            conn.setTimeout(options.idleTimeout!)
            conn.on('timeout', () => {
               if (openConnections.length > options.minConnections!)
                  conn.destroy(Error('Closing connection as it has been idle for too long.'))
            })

            conn.on('close', () => {
               let idx = openConnections.indexOf(conn)
               if (idx > -1) openConnections.splice(idx, 1)

               idx = availableConnections.indexOf(conn)
               if (idx > -1) availableConnections.splice(idx, 1)
            })

            openConnections.push(conn)
            lendConnection(conn)
         })
         .catch(() => {
            if (openConnections.length < options.minConnections!)
               setTimeout(() => tryOpenConnection(Math.min(4096, options.connectTimeout!, retryDelay * 2)), retryDelay)
         })
         .finally(() => openingConnections--)
   }

   function lendConnection(conn: Connection) {
      if (waitingForConnection.length > 0)
         waitingForConnection.shift()!(conn)
      else
         availableConnections.push(conn)
   }

   function borrowConnection<T>(callback: (conn: Connection) => Promise<T> | CancellablePromise<T>): CancellablePromise<T> {
      if (availableConnections.length > 0) {
         const conn = availableConnections.pop()!
         const resultPromise = callback(conn) as CancellablePromise<T>
         resultPromise.finally(() => lendConnection(conn)).catch(() => { })
         if (!resultPromise.cancel)
            resultPromise.cancel = () => { }
         return resultPromise
      }

      if (openConnections.length + openingConnections < options.maxConnections!)
         tryOpenConnection()

      let cancelled = false

      const connPromise = new Promise<Connection>(resolve => waitingForConnection.push(resolve))

      const wrappingPromise = connPromise.then(conn => {
         if (cancelled) {
            lendConnection(conn)
            throw new QueryCancelledError('Query cancelled during connection acquisition phase.')
         }

         const resultPromise = callback(conn) as CancellablePromise<T>
         resultPromise.finally(() => lendConnection(conn)).catch(() => { })
         if (resultPromise.cancel)
            wrappingPromise.cancel = resultPromise.cancel
         return resultPromise
      }) as CancellablePromise<T>

      wrappingPromise.cancel = () => cancelled = true
      return wrappingPromise
   }

   return {
      run<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(query: Query<R, P>): CancellablePromise<QueryResult<R>> {
         return borrowConnection(conn => prepareAndRunQuery(conn, query, options as PoolOptions))
      },
      getQueryMetadata(querySql: string): Promise<QueryMetadata> {
         return borrowConnection(conn => prepareQuery(conn, '', querySql))
      },
      transaction(callback: (client: Client) => Promise<void>): Promise<void> {
         return borrowConnection(async conn => {
            function run<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(query: Query<R, P>): CancellablePromise<QueryResult<R>> {
               return prepareAndRunQuery(conn, query, options as PoolOptions)
            }

            await runSimpleQuery(conn, 'begin')
            return callback({ run })
               .then(() => runSimpleQuery(conn, 'commit'))
               .catch(async err => {
                  await runSimpleQuery(conn, 'rollback')
                  throw err
               })
         })
      },
      destroy() {
         for (const conn of openConnections)
            conn.destroy()

         openingConnections
            = openConnections.length
            = waitingForConnection.length
            = availableConnections.length
            = options.minConnections
            = options.maxConnections
            = 0
      }
   }
}

function openConnection(options: PoolOptions): Promise<Connection> {
   return new Promise((resolve, reject) => {
      const conn = createTcpConnection(options.port, options.host) as Connection

      const timeoutId = setTimeout(
         () => onError(Error('Stopping connection attempt as it has been going on for too long.')),
         options.connectTimeout
      )

      if (options.ssl) {
         conn.once('connect', () => conn.write(sslRequestMessage))
         conn.once('data', data => {
            if (isSslSuppported(data)) {
               createTlsConnection({ socket: conn, ...options.ssl })
               initStartupPhase()
            }
            else
               onError(Error('Postgres server does not support SSL.'))
         })
      }
      else
         conn.once('connect', () => initStartupPhase())

      conn.once('error', onError)
      conn.once('close', () => onError(Error('Connection has been closed.')))

      function initStartupPhase() {
         const promise = handleStartupPhase(conn, options.username, options.password)
         conn.write(createStartupMessage(options.username, options.database))
         promise
            .then(({ data }) => {
               clearTimeout(timeoutId)
               conn.cancelKey = data
               conn.preparedQueries = {}
               resolve(conn)
            })
            .catch(({ error }: BackendFailure) => onError(error))
      }

      function onError(err: Error) {
         console.error(err)
         reject(err)
         conn.destroy(err)
      }
   })
}

function runSimpleQuery(conn: Connection, query: 'begin' | 'commit' | 'rollback' | `savepoint ${string}` | `rollback to ${string}` | `release ${string}`) {
   const promise = handleSimpleQueryExecution(conn)
   conn.write(createSimpleQueryMessage(query))
   return promise as any as Promise<void> // FIXME
}

function prepareAndRunQuery<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(conn: Connection, query: Query<R, P>, options: PoolOptions): CancellablePromise<QueryResult<R>> {
   let cancelled = false
   let cancelledPromise: Promise<void>

   function cancel() {
      cancelled = true
      cancelledPromise = cancelCurrentQuery(conn, options)
   }

   const timeoutId = setTimeout(cancel, options.queryTimeout)

   const resultPromise = (async () => {
      try {
         query.id = query.id || ''
         query.params = query.params || []
         query.metadata = query.metadata || await prepareQuery(conn, query.id!, query.sql)
         if (cancelled) {
            try { await cancelledPromise! } catch {}
            throw new QueryCancelledError('Query cancelled during query preparation phase.')
         }
         const queryResult = await runExtendedQuery<R>(conn, query as Required<Query>)
         if (cancelled) {
            try { await cancelledPromise! } catch {}
            throw new QueryCancelledError('Query cancelled during query execution phase.')
         }
         return queryResult
      } catch (err) {
         if (cancelled) {
            try { await cancelledPromise! } catch {}
            if (!(err instanceof QueryCancelledError))
               throw new QueryCancelledError((err as Error).message)
         }
         throw err
      } finally {
         clearTimeout(timeoutId)
      }
   })() as CancellablePromise<QueryResult<R>>

   resultPromise.cancel = cancel
   return resultPromise
}

function prepareQuery(conn: Connection, queryId: string, querySql: string) {
   const { preparedQueries } = conn
   if (queryId && preparedQueries[queryId])
      return Promise.resolve(preparedQueries[queryId])

   const promise = handleQueryPreparation(conn).then(({ data }) => {
      if (queryId)
         preparedQueries[queryId] = data

      return data // FIXME
   })

   conn.write(Buffer.concat([
      createParseMessage(querySql, queryId, []),
      createDescribeMessage(DescribeType.PreparedStatement, queryId),
      syncMessage
   ]))

   return promise
}

function runExtendedQuery<R extends Row>(conn: Connection, query: Required<Query>): Promise<QueryResult<R>> {
   let preparedQuery: QueryMetadata | undefined
   const { preparedQueries } = conn
   if (query.id && preparedQueries[query.id])
     preparedQuery = preparedQueries[query.id]

   const promise = handleQueryExecution<R>(conn, query.metadata.rowMetadata, preparedQuery ? true : false).then(({ data }) => {
      if (query.id)
         preparedQueries[query.id] = query.metadata

      return data // FIXME
   })

   conn.write(Buffer.concat([
     preparedQuery ? Buffer.of() : createParseMessage(query.sql, query.id, []),
     createBindMessage(query.id, query.params, query.metadata.paramTypes, ''),
     executeUnnamedPortalMessage,
     syncMessage
   ]))

   return promise
}

function cancelCurrentQuery(conn: Connection, options: PoolOptions): Promise<void> {
   return new Promise((resolve, reject) => {
      const cancelConn = createTcpConnection(options.port, options.host)

      cancelConn.once('error', err => {
         reject(err)
         cancelConn.destroy(err)
      })

      cancelConn.once('connect', () =>
         cancelConn.write(conn.cancelKey, 'utf8', err => {
            if (err) {
               reject(err)
               cancelConn.destroy(err)
            } else {
               // An error may still be emitted, see https://nodejs.org/api/stream.html#stream_writable_write_chunk_encoding_callback
               setTimeout(resolve)
            }
         })
      )
   })
}
