import { createHash } from 'crypto'
import { createConnection as createTcpConnection, Socket } from 'net'
import { connect as createTlsConnection, ConnectionOptions } from 'tls'

// References:
// - https://postgresql.org/docs/13/protocol.html
// - https://postgresql.org/docs/current/datatype.html
// - https://beta.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf
// - https://github.com/postgres/postgres/tree/master/src/backend/libpq
// - https://github.com/postgres/postgres/tree/master/src/backend/utils/adt

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
  run<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(query: Query<P, R>): CancellablePromise<QueryResult<R>>
}

export interface Pool extends Client {
  getQueryMetadata(query: string): Promise<QueryMetadata>
  transaction(callback: (client: Client) => Promise<void>): Promise<void>
  destroy(): void
}

export interface Query<P extends ColumnValue[], _R extends Row = Row> {
  sql: string
  params?: P
  paramTypes?: ObjectId[]
  id?: string
}

export interface QueryResult<R extends Row> {
  rows: R[]
  rowsAffected: number
}

export interface QueryMetadata {
  paramTypes: ObjectId[]
  rowMetadata: ColumnMetadata[]
}

export type Row = {
  [columnName: string]: ColumnValue
}

export type ColumnValue = any // undefined | null | boolean | number | number[] | bigint | bigint[] | string | string[] | Date | Buffer

export interface ColumnMetadata {
  name: string
  type: ObjectId
  tableId?: number
  positionInTable?: number
}

export interface CancellablePromise<T> extends Promise<T> {
  cancel(): void
}

export class QueryCancelledError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'CancelError'
  }
}

export function sql<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(queryTextParts: TemplateStringsArray, ...params: P): Query<P, R> {
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

  let queryText = ''
  for (let i = 0; i < lastIdx; ++i) {
    queryText += queryTextParts[i] + '$' + argIndices[i]
  }
  queryText += queryTextParts[lastIdx]

  return {
    sql: queryText,
    params: uniqueParams,
    id: md5(queryText)
  }
}

interface Connection extends Socket {
  processId: number
  cancelKey: number
  preparedQueries: {
    [queryId: string]: QueryMetadata
  }
}

export function newPool(options: Partial<PoolOptions> = {}): Pool {
  const { env } = process

  // Environment variables read from the following sources:
  // 1. https://www.postgresql.org/docs/current/libpq-envars.html
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

  const connCount = { value: 0 }
  const connPool: Promise<Connection>[] = []
  const connQueue: ((connPromise: Promise<Connection>) => void)[] = []

  for (let i = connCount.value; i < options.minConnections; ++i) {
    putConnectionInPool()
  }

  function putConnectionInPool(retryDelay = 1) {
    const connPromise = openConnection(options as PoolOptions, connCount)
    connPromise.catch(() => {
      const idx = connPool.indexOf(connPromise)
      if (idx > -1) {
        connPool.splice(idx, 1)
      }
      connCount.value--
      if (connCount.value < options.minConnections!) {
        setTimeout(() => putConnectionInPool(Math.min(1024, options.connectTimeout!, retryDelay * 2)), retryDelay)
      }
    })
    connCount.value++
    onConnectionAvailable(connPromise)
  }

  function takeConnectionFromPool(): Promise<Connection> {
    if (connPool.length === 0 && connCount.value < options.maxConnections!) {
      putConnectionInPool()
    }
    return connPool.length > 0
      ? connPool.pop()!
      : new Promise(resolve => connQueue.push(resolve))
  }

  function onConnectionAvailable(connPromise: Promise<Connection>) {
    if (connQueue.length > 0) {
      connQueue.shift()!(connPromise)
    } else {
      connPool.push(connPromise)
    }
  }

  function withConnection<T>(callback: (conn: Connection) => Promise<T>): CancellablePromise<T> {
    let cancelled = false
    const connPromise = takeConnectionFromPool()

    const resultPromise = connPromise
      .then(conn => {
        if (cancelled) {
          throw new QueryCancelledError('Query cancelled before query preparation phase.')
        }
        return callback(conn)
      })
      .catch(err => {
        if (cancelled && !(err instanceof QueryCancelledError)) {
          throw new QueryCancelledError('Query cancelled during connection acquisition phase.')
        }
        throw err
      })
      .finally(() => {
        if (!cancelled) {
          onConnectionAvailable(connPromise)
        }
      }) as CancellablePromise<T>

    resultPromise.cancel = () => {
      cancelled = true
      onConnectionAvailable(connPromise)
    }

    return resultPromise
  }

  function prepareAndRunQuery<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(conn: Connection, query: Query<P, R>): CancellablePromise<QueryResult<R>> {
    let cancelled = false
    let cancelledPromise: Promise<void>

    const queryTimeoutId = setTimeout(() => resultPromise.cancel(), options.queryTimeout)

    const resultPromise = prepareQuery(conn, query.id || '', query.sql, query.paramTypes)
      .then(preparedQuery => {
        if (cancelled) {
          throw new QueryCancelledError('Query cancelled before query execution phase.')
        }
        return runPreparedQuery<R>(conn, query.id || '', query.params || [], preparedQuery.paramTypes, preparedQuery.rowMetadata)
          .then(res => {
            if (cancelled) {
              throw new QueryCancelledError('Query cancelled after query execution phase.')
            }
            return res
          })
          .catch(err => {
            if (cancelled && !(err instanceof QueryCancelledError)) {
              throw new QueryCancelledError('Query cancelled during query execution phase.')
            }
            throw err
          })
      })
      .catch(err => {
        if (cancelled && !(err instanceof QueryCancelledError)) {
          throw new QueryCancelledError('Query cancelled during query preparation phase.')
        }
        throw err
      })
      .finally(async () => {
        clearTimeout(queryTimeoutId)

        if (cancelled) {
          await cancelledPromise
        }
      }) as CancellablePromise<QueryResult<R>>

    resultPromise.cancel = () => {
      cancelled = true
      cancelledPromise = cancelCurrentQuery(conn, options as PoolOptions)
    }

    return resultPromise
  }

  return {
    run<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(query: Query<P, R>): CancellablePromise<QueryResult<R>> {
      return withConnection(conn => prepareAndRunQuery(conn, query))
    },
    getQueryMetadata(querySql: string): Promise<QueryMetadata> {
      return withConnection(conn => prepareQuery(conn, '', querySql, undefined))
    },
    transaction(callback: (client: Client) => Promise<void>): Promise<void> {
      return withConnection(async conn => {
        function run<R extends Row = Row, P extends ColumnValue[] = ColumnValue[]>(query: Query<P, R>): CancellablePromise<QueryResult<R>> {
          return prepareAndRunQuery(conn, query)
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
      const connPoolCopy = [...connPool]
      options.minConnections
        = options.maxConnections
        = connQueue.length
        = connPool.length
        = 0
      for (const connPromise of connPoolCopy) {
        connPromise.then(conn => conn.destroy())
      }
    }
  }
}

function openConnection(options: PoolOptions, connCount: { value: number }): Promise<Connection> {
  return new Promise(async (resolve, reject) => {
    const connectTimeoutId = setTimeout(
      () => handleStartupPhaseError(Error('Stopping connection attempt as it has been going on for too long.')),
      options.connectTimeout
    )

    const conn = createTcpConnection(options.port, options.host) as Connection

    function handleStartupPhaseError(err: Error) {
      reject(err)
      conn?.destroy(err)
    }

    if (options.ssl) {
      conn.once('connect', () => {
        conn.write(sslRequestMessage)
        conn.on('data', handleStartupPhase)
      })
      conn.once('data', data => {
        if (readUint8(data, 0) === 83) { // 'S'
          createTlsConnection({ socket: conn, ...options.ssl })
        } else {
          handleStartupPhaseError(Error('Postgres server does not support SSL.'))
        }
      })
    } else {
      conn.once('connect', () => {
        conn.write(createStartupMessage(options.username, options.database))
        conn.on('data', handleStartupPhase)
      })
    }

    conn.on('error', handleStartupPhaseError)

    conn.setTimeout(options.idleTimeout)
    conn.on('timeout', () => {
      if (connCount.value > options.minConnections) {
        handleStartupPhaseError(Error('Closing connection as it has been idle for too long.'))
      }
    })

    conn.on('close', () => handleStartupPhaseError(Error('Connection has been closed.')))

    conn.on('data', data => {
      const msgType = readUint8(data, 0) as BackendMessage
      if (msgType === BackendMessage.NoticeResponse) {
        const notice = parseErrorResponse(data)
        console.log('[NOTICE]', notice)
      }
    })

    let authOk = false

    function handleStartupPhase(data: Buffer): void {
      const msgType = readUint8(data, 0) as BackendMessage

      if (msgType === BackendMessage.Authentication) {
        const authRes = readInt32(data, 5) as AuthenticationResponse
        if (authRes === AuthenticationResponse.Md5Password) {
          const salt = data.slice(9)
          conn.write(createMd5PasswordMessage(options.username, options.password, salt))
        } else if (authRes === AuthenticationResponse.Ok) {
          authOk = true
        } else if (authRes === AuthenticationResponse.CleartextPassword) {
          conn.write(createCleartextPasswordMessage(options.password))
        } else {
          return handleStartupPhaseError(Error(`Unsupported authentication response sent by server: "${AuthenticationResponse[authRes] || authRes}".`))
        }
      }
      else if (msgType === BackendMessage.ParameterStatus) {
        // const paramName = readCString(data, 5)
        // const paramValue = readCString(data, 5 + paramName.length + 1)
      }
      else if (msgType === BackendMessage.BackendKeyData) {
        conn.processId = readInt32(data, 5)
        conn.cancelKey = readInt32(data, 9)
      }
      else if (msgType === BackendMessage.ReadyForQuery) {
        if (authOk) {
          clearTimeout(connectTimeoutId)
          conn.removeListener('data', handleStartupPhase)
          conn.preparedQueries = {}
          resolve(conn)
        } else {
          return handleStartupPhaseError(Error('Authentication could not be completed.'))
        }
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        const msg = parseErrorResponse(data).find(err => err.type === ErrorResponseType.Message)?.value || ''
        return handleStartupPhaseError(Error(`Error received from server during startup phase: "${msg}".`))
      }
      else if (msgType === BackendMessage.NegotiateProtocolVersion) {
        const minorVersion = readInt32(data, 5)
        const unrecognizedOptions: string[] = []
        const unrecognizedOptionsCount = readInt32(data, 9)
        let offset = 13
        for (let i = 0; i < unrecognizedOptionsCount; ++i) {
          const opt = readCString(data, offset)
          unrecognizedOptions.push(opt)
          offset += opt.length
        }
        const unrecognizedOptionsMsg = unrecognizedOptions.length === 0 ? '' : ` The following options were not recognized by the server: ${unrecognizedOptions.join(', ')}.`
        return handleStartupPhaseError(Error(`The Postgres server does not support protocol versions greather than 3.${minorVersion}.${unrecognizedOptionsMsg}`))
      }
      else {
        console.warn(`[WARN] Unexpected message type sent by server during startup phase: "${BackendMessage[msgType] || msgType}".`)
      }

      const msgSize = 1 + readInt32(data, 1)
      if (data.byteLength > msgSize) {
        handleStartupPhase(data.slice(msgSize))
      }
    }
  })
}

function runSimpleQuery(conn: Connection, query: 'begin' | 'commit' | 'rollback'): Promise<void> {
  return new Promise((resolve, reject) => {
    let leftover: Buffer | undefined
    let commandCompleted = false

    conn.on('data', handleSimpleQueryExecution)
    conn.write(createSimpleQueryMessage(query))

    function handleSimpleQueryExecution(data: Buffer): void {
      if (leftover) {
        data = Buffer.concat([leftover, data])
        leftover = undefined
      }

      if (data.byteLength <= 5) {
        leftover = data
        return
      }

      const msgSize = 1 + readInt32(data, 1)
      if (msgSize > data.byteLength) {
        leftover = data
        return
      }

      const msgType = readUint8(data, 0) as BackendMessage
      if (msgType === BackendMessage.CommandComplete) {
        commandCompleted = true
      }
      else if (msgType === BackendMessage.ReadyForQuery) {
        conn.removeListener('data', handleSimpleQueryExecution)
        if (commandCompleted) {
          return resolve()
        } else {
          return reject(Error('Failed to execute simple query.'))
        }
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        const msg = parseErrorResponse(data).find(err => err.type === ErrorResponseType.Message)?.value || ''
        const err = Error(`Error received from server during simple query execution phase: "${msg}".`)
        return reject(err)
      }
      else if (msgType === BackendMessage.EmptyQueryResponse) {
        return reject(Error('Empty query received.'))
      }
      else {
        console.warn(`[WARN] Unexpected message received during simple query execution phase: ${BackendMessage[msgType] || msgType}.`)
      }

      if (data.byteLength > msgSize) {
        handleSimpleQueryExecution(data.slice(msgSize))
      }
    }
  })
}

function prepareQuery(conn: Connection, queryId: string, querySql: string, paramTypes: ObjectId[] | undefined): Promise<QueryMetadata> {
  const { preparedQueries } = conn
  if (queryId && preparedQueries[queryId]) {
    return Promise.resolve(preparedQueries[queryId])
  }

  return new Promise((resolve, reject) => {
    const shouldFetchParamTypes = paramTypes === undefined
    if (shouldFetchParamTypes) {
      paramTypes = []
    }

    let leftover: Buffer | undefined
    let parseCompleted = false
    let paramTypesFetched = false
    let rowMetadataFetched = false

    const rowMetadata: ColumnMetadata[] = []

    conn.on('data', handleQueryPreparation)
    conn.write(Buffer.concat([
      createParseMessage(querySql, queryId, paramTypes!),
      createDescribeMessage(queryId),
      syncMessage
    ]))

    function handleQueryPreparation(data: Buffer): void {
      if (leftover) {
        data = Buffer.concat([leftover, data])
        leftover = undefined
      }

      if (data.byteLength <= 5) {
        leftover = data
        return
      }

      const msgSize = 1 + readInt32(data, 1)
      if (msgSize > data.byteLength) {
        leftover = data
        return
      }

      const msgType = readUint8(data, 0) as BackendMessage
      if (msgType === BackendMessage.ParseComplete) {
        parseCompleted = true
      }
      else if (msgType === BackendMessage.ParameterDescription) {
        if (shouldFetchParamTypes) {
          const paramCount = readInt16(data, 5)
          let offset = 7
          for (let i = 0; i < paramCount; ++i) {
            const paramType = readInt32(data, offset)
            offset += 4
            paramTypes!.push(paramType)
          }
          paramTypesFetched = true
        }
      }
      else if (msgType === BackendMessage.RowDescription) {
        const colCount = readInt16(data, 5)
        let offset = 7
        for (let i = 0; i < colCount; ++i) {
          const name            = readCString(data, offset)             ; offset += name.length + 1
          const tableId         = readInt32(data, offset) || undefined  ; offset += 4
          const positionInTable = readInt16(data, offset) || undefined  ; offset += 2
          const type            = readInt32(data, offset)               ; offset += 4
          /* const typeSize        = readInt16(data, offset)               ; */ offset += 2
          /* const typeModifier    = readInt32(data, offset)               ; */ offset += 4
          /* const format          = readInt16(data, offset) as WireFormat ; */ offset += 2
          rowMetadata.push({ name, type, tableId, positionInTable })
        }
        rowMetadataFetched = true
      }
      else if (msgType === BackendMessage.ReadyForQuery) {
        conn.removeListener('data', handleQueryPreparation)
        if (parseCompleted && (!shouldFetchParamTypes || paramTypesFetched) && rowMetadataFetched) {
          const queryMetadata = { paramTypes: paramTypes!, rowMetadata }
          if (queryId) preparedQueries[queryId] = queryMetadata
          return resolve(queryMetadata)
        } else {
          return reject(Error('Failed to parse query.'))
        }
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        const msg = parseErrorResponse(data).find(err => err.type === ErrorResponseType.Message)?.value || ''
        const err = Error(`Error received from server during query preparation phase: "${msg}".`)
        return reject(err)
      }
      else if (msgType === BackendMessage.NoData || msgType === BackendMessage.BindComplete || msgType === BackendMessage.CommandComplete) {
        const queryMetadata = { paramTypes: [], rowMetadata: [] }
        if (queryId) preparedQueries[queryId] = queryMetadata
        return resolve(queryMetadata)
      }
      else {
        console.warn('[WARN] Unexpected message received during query preparation phase: ' + (BackendMessage[msgType] || msgType))
      }

      if (data.byteLength > msgSize) {
        handleQueryPreparation(data.slice(msgSize))
      }
    }
  })
}

function cancelCurrentQuery(conn: Connection, options: PoolOptions): Promise<void> {
  return new Promise((resolve, reject) => {
    const cancelConn = createTcpConnection(options.port, options.host)

    cancelConn.on('connect', () => cancelConn.write(createCancelRequestMessage(conn.processId, conn.cancelKey), 'utf8', err => {
      if (err) {
        cancelConn.destroy(err)
        reject(err)
      } else {
        resolve()
      }
    }))

    cancelConn.on('error', err => {
      cancelConn.destroy(err)
      reject(err)
    })
  })
}

const commandsWithRowsAffected = ['INSERT', 'DELETE', 'UPDATE', 'SELECT', 'MOVE', 'FETCH', 'COPY']

function runPreparedQuery<R extends Row>(conn: Connection, queryId: string, params: ColumnValue[], paramTypes: ObjectId[], rowMetadata: ColumnMetadata[]): Promise<QueryResult<R>> {
  return new Promise((resolve, reject) => {
    let leftover: Buffer | undefined
    let bindingCompleted = false
    let commandCompleted = false

    const rows: R[] = []
    let rowsAffected = 0

    conn.on('data', handleQueryExecution)
    conn.write(Buffer.concat([
      createBindMessage(queryId, params, paramTypes, ''),
      executeUnnamedPortalMessage,
      syncMessage
    ]))

    function handleQueryExecution(data: Buffer): void {
      if (leftover) {
        data = Buffer.concat([leftover, data])
        leftover = undefined
      }

      if (data.byteLength <= 5) {
        leftover = data
        return
      }

      const msgSize = 1 + readInt32(data, 1)
      if (msgSize > data.byteLength) {
        leftover = data
        return
      }

      const msgType = readUint8(data, 0) as BackendMessage
      if (msgType === BackendMessage.DataRow) {
        const paramCount = readInt16(data, 5)
        if (rowMetadata.length !== paramCount) {
          console.warn(`[WARN] Received ${paramCount} query parameters, but ${rowMetadata.length} column descriptions.`)
          return
        }

        const row: Row = {}
        let offset = 7
        for (let i = 0; i < paramCount; ++i) {
          const column = rowMetadata[i]
          const valueSize = readInt32(data, offset)
          offset += 4
          if (valueSize === -1) {
            row[column.name] = null
            continue
          }
          const value = data.slice(offset, offset += valueSize)
          switch (column.type) {
          case ObjectId.Bool:        row[column.name] = value[0] !== 0          ; break
          case ObjectId.Int2:        row[column.name] = readInt16(value, 0)     ; break
          case ObjectId.Int4:        row[column.name] = readInt32(value, 0)     ; break
          case ObjectId.Int8:        row[column.name] = readInt64(value, 0)     ; break
          case ObjectId.Float4:      row[column.name] = readFloat32(value, 0)   ; break
          case ObjectId.Float8:      row[column.name] = readFloat64(value, 0)   ; break
          case ObjectId.Numeric:     row[column.name] = readNumeric(value, 0)   ; break
          case ObjectId.Timestamp:
          case ObjectId.Timestamptz: row[column.name] = readTimestamp(value, 0) ; break
          case ObjectId.Oid:
          case ObjectId.Regproc:     row[column.name] = readUint32(value, 0)    ; break
          case ObjectId.Char:
          case ObjectId.Varchar:
          case ObjectId.Text:
          case ObjectId.Bpchar:
          case ObjectId.Name:        row[column.name] = value.toString('utf8')  ; break
          case ObjectId.CharArray:
          case ObjectId.VarcharArray:
          case ObjectId.TextArray:
          case ObjectId.BpcharArray:
          case ObjectId.NameArray:   row[column.name] = readArray(value, readUtf8String)            ; break
          case ObjectId.Int2Array:   row[column.name] = readArray(value, readInt16)                 ; break
          case ObjectId.Int4Array:   row[column.name] = readArray(value, readInt32)                 ; break
          case ObjectId.Int8Array:   row[column.name] = readArray(value, readInt64)                 ; break
          case ObjectId.Float4Array: row[column.name] = readArray(value, readFloat32)               ; break
          case ObjectId.Float8Array: row[column.name] = readArray(value, readFloat64)               ; break
          case ObjectId.Bytea:       row[column.name] = value                                       ; break
          case ObjectId.Jsonb:       row[column.name] = JSON.parse(value.slice(1).toString('utf8')) ; break
          case ObjectId.Json:        row[column.name] = JSON.parse(value.toString('utf8'))          ; break
          default:
            console.warn(`[WARN] Unsupported column data type: ${ObjectId[column.type] || column.type}.`)
            row[column.name] = value
          }
        }
        rows.push(row as R)
      }
      else if (msgType === BackendMessage.BindComplete) {
        bindingCompleted = true
      }
      else if (msgType === BackendMessage.CommandComplete) {
        const commandTagParts = readCString(data, 5).split(' ')
        const commandTag = commandTagParts[0]
        if (commandsWithRowsAffected.indexOf(commandTag) > -1)  {
          rowsAffected = parseInt(commandTagParts[commandTagParts.length - 1], 10)
        }
        commandCompleted = true
      }
      else if (msgType === BackendMessage.ReadyForQuery) {
        // const txStatus = readUint8(data, 5) as TransactionStatus
        conn.removeListener('data', handleQueryExecution)
        if (bindingCompleted && commandCompleted) {
          return resolve({ rows, rowsAffected })
        } else {
          return reject(Error('Failed to execute prepared query.'))
        }
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        const msg = parseErrorResponse(data).find(err => err.type === ErrorResponseType.Message)?.value || ''
        const err = Error(`Error received from server during prepared query execution phase: "${msg}".`)
        return reject(err)
      }
      else if (msgType === BackendMessage.EmptyQueryResponse) {
        return reject(Error('Empty query received.'))
      }
      else {
        console.warn(`[WARN] Unexpected message received during prepared query execution phase: ${BackendMessage[msgType] || msgType}.`)
      }

      if (data.byteLength > msgSize) {
        handleQueryExecution(data.slice(msgSize))
      }
    }
  })
}

interface ParsedError {
  type: ErrorResponseType
  value: string
}

function parseErrorResponse(data: Buffer): ParsedError[] {
  const msgSize = 1 + readInt32(data, 1)
  const errors: ParsedError[] = []
  let offset = 5
  while (offset < msgSize) {
    const type = readUint8(data, offset++) as ErrorResponseType
    if (type !== 0) {
      const value = readCString(data, offset)
      offset += Buffer.byteLength(value) + 1
      errors.push({ type, value })
    }
  }
  return errors
}

const sslRequestMessage = createSslRequestMessage()

function createSslRequestMessage(): Buffer {
  // 8 = 4 (message size) + 4 (SSL request code)
  const size = 8
  const message = Buffer.allocUnsafe(size)
  writeInt32(message, size, 0)
  writeInt32(message, 8087_7103, 4)
  return message
}

function createStartupMessage(username: string, database: string): Buffer {
  // 15 = 4 (message size) + 4 (protocol version) + 5 ("user" and null terminator) + 1 (username null terminator) + 1 (additional null terminator)
  let size = 15 + Buffer.byteLength(username)

  const differentNames = database !== username
  if (differentNames) {
    // 10 = 9 ("database" and null terminator) + 1 (database null terminator)
    size += 10 + Buffer.byteLength(database)
  }

  const message = Buffer.allocUnsafe(size)
  let offset = 0

  offset = writeInt32(message, size, offset)
  offset = writeInt32(message, 196_608, offset) // Protocol version 3.0
  offset = writeCString(message, 'user', offset)
  offset = writeCString(message, username, offset)

  if (differentNames) {
    offset = writeCString(message, 'database', offset)
    offset = writeCString(message, database, offset)
  }

  writeUint8(message, 0, offset)

  return message
}

function createCancelRequestMessage(processId: number, cancelKey: number): Buffer {
  const size = 16
  const message = Buffer.allocUnsafe(size)
  writeInt32(message, size, 0)
  writeInt32(message, 80877102, 4) // Cancel request code
  writeInt32(message, processId, 8)
  writeInt32(message, cancelKey, 12)
  return message
}

function createCleartextPasswordMessage(password: string) {
  // 6 = 1 (message type) + 4 (message size) + 1 (password null terminator)
  const size = 6 + Buffer.byteLength(password)
  const message = Buffer.allocUnsafe(size)
  writeUint8(message, FrontendMessage.PasswordMessage, 0)
  writeInt32(message, size - 1, 1)
  writeCString(message, password, 5)
  return message
}

function createMd5PasswordMessage(username: string, password: string, salt: Buffer) {
  const credentialsMd5 = 'md5' + md5(Buffer.concat([Buffer.from(md5(password + username)), salt]))

  // 6 = 1 (message type) + 4 (message size) + 1 (credentialsMd5 null terminator)
  const size = 6 + Buffer.byteLength(credentialsMd5)
  const message = Buffer.allocUnsafe(size)

  writeUint8(message, FrontendMessage.PasswordMessage, 0)
  writeInt32(message, size - 1, 1)
  writeCString(message, credentialsMd5, 5)

  return message
}

function md5(x: string | Buffer) {
  return createHash('md5').update(x).digest('hex')
}

function createSimpleQueryMessage(querySql: string): Buffer {
  // 6 = 1 (message type) + 4 (message size) + 1 (query null terminator)
  const size = 6 + Buffer.byteLength(querySql)
  const message = Buffer.allocUnsafe(size)
  writeUint8(message, FrontendMessage.Query, 0)
  writeInt32(message, size - 1, 1)
  writeCString(message, querySql, 5)
  return message
}

function createParseMessage(querySql: string, queryId: string, paramTypes: ObjectId[]): Buffer {
  // 9 = 1 (message type) + 4 (message size) + 1 (queryId null terminator) + 1 (query null terminator) + 2 (number of parameter data)
  const size = 9 + Buffer.byteLength(queryId) + Buffer.byteLength(querySql) + paramTypes.length * 4
  const message = Buffer.allocUnsafe(size)
  let offset = 0

  offset = writeUint8(message, FrontendMessage.Parse, offset)
  offset = writeInt32(message, size - 1, offset)
  offset = writeCString(message, queryId, offset)
  offset = writeCString(message, querySql, offset)
  offset = writeInt16(message, paramTypes.length, offset)

  for (const t of paramTypes) {
    offset = writeInt32(message, t, offset)
  }

  return message
}

function createDescribeMessage(queryId: string): Buffer {
  // 7 = 1 (message type) + 4 (message size) + 1 (describe message type) + 1 (queryId null terminator)
  const size = 7 + Buffer.byteLength(queryId)
  const message = Buffer.allocUnsafe(size)
  writeUint8(message, FrontendMessage.Describe, 0)
  writeInt32(message, size - 1, 1)
  writeUint8(message, DescribeOrCloseRequest.PreparedStatement, 5)
  writeCString(message, queryId, 6)
  return message
}

const syncMessage = createSyncMessage()

function createSyncMessage(): Buffer {
  // 5 = 1 (message type) + 4 (message size)
  const size = 5
  const message = Buffer.allocUnsafe(size)
  writeUint8(message, FrontendMessage.Sync, 0)
  writeInt32(message, size - 1, 1)
  return message
}

function createBindMessage(queryId: string, params: ColumnValue[], paramTypes: ObjectId[], portal: string): Buffer {
  let bufferSize
    = 1 // Message type
    + 4 // Message size
    + Buffer.byteLength(portal) + 1
    + Buffer.byteLength(queryId) + 1
    + 2 // Number of parameter format codes
    + 2 // Parameter format code(s)
    + 2 // Number of parameters
      + 4 * params.length // Length of the parameter values
      + 0 // Values of the parameters (see below)
    + 2 // Number of result-column format codes
    + 2 // Result-column format code(s)

  for (let i = 0; i < params.length; ++i) {
    const v = params[i]
    if (v == null) {
      continue
    }
    switch (paramTypes[i]) {
    case ObjectId.Bool:         bufferSize += 1 ; break
    case ObjectId.Int2:         bufferSize += 2 ; break
    case ObjectId.Int4:
    case ObjectId.Float4:
    case ObjectId.Oid:
    case ObjectId.Regproc:      bufferSize += 4 ; break
    case ObjectId.Int8:
    case ObjectId.Float8:
    case ObjectId.Timestamp:
    case ObjectId.Timestamptz:  bufferSize += 8 ; break
    case ObjectId.Char:
    case ObjectId.Varchar:
    case ObjectId.Text:
    case ObjectId.Bpchar:
    case ObjectId.Name:         bufferSize += Buffer.byteLength(v as string)                                                 ; break
    // TODO Find cheaper way to calculate this (try to avoid the whole switch altogether).
    case ObjectId.Numeric:      bufferSize += writeNumeric(Buffer.allocUnsafe(8 + 2 * (v as string).length), v as string, 0) ; break
    case ObjectId.Bytea:        bufferSize += (v as Buffer).length                                                           ; break
    case ObjectId.Json:         bufferSize += JSON.stringify(v).length                                                       ; break
    case ObjectId.Jsonb:        bufferSize += 1 + JSON.stringify(v).length                                                   ; break
    case ObjectId.CharArray:
    case ObjectId.VarcharArray:
    case ObjectId.TextArray:
    case ObjectId.BpcharArray:
    case ObjectId.NameArray:    bufferSize += 20 + byteLengthSum(v as string[]) ; break
    case ObjectId.Int2Array:    bufferSize += 20 + (v as number[]).length * 6   ; break
    case ObjectId.Int4Array:
    case ObjectId.Float4Array:  bufferSize += 20 + (v as number[]).length * 8   ; break
    case ObjectId.Int8Array:
    case ObjectId.Float8Array:  bufferSize += 20 + (v as number[]).length * 12  ; break
    default:
      throw Error(`Tried binding a parameter of an unsupported type: ${ObjectId[paramTypes[i]] || paramTypes[i]}`)
    }
  }

  const message = Buffer.allocUnsafe(bufferSize)
  let offset = 0

  offset = writeUint8(message, FrontendMessage.Bind, offset)
  offset = writeInt32(message, bufferSize - 1, offset)
  offset = writeCString(message, portal, offset)
  offset = writeCString(message, queryId, offset)
  offset = writeInt16(message, 1, offset)
  offset = writeInt16(message, WireFormat.Binary, offset)
  offset = writeInt16(message, params.length, offset)

  for (let i = 0; i < params.length; ++i) {
    const v = params[i]
    if (v == null) {
      offset = writeInt32(message, -1, offset)
      continue
    }

    let sizeOffset = offset
    offset += 4

    switch (paramTypes[i]) {
    case ObjectId.Bool:        offset = writeUint8(message, +(v as boolean), offset) ; break
    case ObjectId.Int2:        offset = writeInt16(message, v as number, offset)     ; break
    case ObjectId.Int4:        offset = writeInt32(message, v as number, offset)     ; break
    case ObjectId.Oid:
    case ObjectId.Regproc:     offset = writeUint32(message, v as number, offset)    ; break
    case ObjectId.Int8:        offset = writeInt64(message, v as bigint, offset)     ; break
    case ObjectId.Float4:      offset = writeFloat32(message, v as number, offset)   ; break
    case ObjectId.Float8:      offset = writeFloat64(message, v as number, offset)   ; break
    case ObjectId.Timestamp:
    case ObjectId.Timestamptz: offset = writeTimestamp(message, v as Date, offset)   ; break
    case ObjectId.Char:
    case ObjectId.Varchar:
    case ObjectId.Text:
    case ObjectId.Bpchar:
    case ObjectId.Name:         offset = writeUtf8String(message, v as string, offset)                                 ; break
    case ObjectId.Numeric:      offset = writeNumeric(message, v as string, offset)                                    ; break
    case ObjectId.Bytea:        offset += (v as Buffer).copy(message, offset)                                          ; break
    case ObjectId.Json:         offset = writeUtf8String(message, JSON.stringify(v), offset)                           ; break
    case ObjectId.Jsonb:        offset = writeUtf8String(message, String.fromCharCode(1) + JSON.stringify(v), offset)  ; break
    case ObjectId.CharArray:    offset = writeArray(message, v as string[], offset, ObjectId.Char, writeUtf8String)    ; break
    case ObjectId.VarcharArray: offset = writeArray(message, v as string[], offset, ObjectId.Varchar, writeUtf8String) ; break
    case ObjectId.TextArray:    offset = writeArray(message, v as string[], offset, ObjectId.Text, writeUtf8String)    ; break
    case ObjectId.BpcharArray:  offset = writeArray(message, v as string[], offset, ObjectId.Bpchar, writeUtf8String)  ; break
    case ObjectId.NameArray:    offset = writeArray(message, v as string[], offset, ObjectId.Name, writeUtf8String)    ; break
    case ObjectId.Int2Array:    offset = writeArray(message, v as number[], offset, ObjectId.Int2, writeInt16)         ; break
    case ObjectId.Int4Array:    offset = writeArray(message, v as number[], offset, ObjectId.Int4, writeInt32)         ; break
    case ObjectId.Int8Array:    offset = writeArray(message, v as bigint[], offset, ObjectId.Int8, writeInt64)         ; break
    case ObjectId.Float4Array:  offset = writeArray(message, v as number[], offset, ObjectId.Float4, writeFloat32)     ; break
    case ObjectId.Float8Array:  offset = writeArray(message, v as number[], offset, ObjectId.Float8, writeFloat64)     ; break
    default:
      throw Error(`Tried binding a parameter of an unsupported type: ${ObjectId[paramTypes[i]] || paramTypes[i]}`)
    }

    writeInt32(message, offset - (sizeOffset + 4), sizeOffset)
  }

  offset = writeInt16(message, 1, offset)
  offset = writeInt16(message, WireFormat.Binary, offset)

  return message
}

function byteLengthSum(arr: string[]): number {
  for (var s = 0, i = 0; i < arr.length; ++i) {
    s += Buffer.byteLength(arr[i])
  }
  return s
}

const executeUnnamedPortalMessage = createExecuteMessage('')

function createExecuteMessage(portal: string): Buffer {
  // 10 = 1 (message type) + 4 (message size) + 1 (portal null terminator) + 4 (maximum number of rows to return)
  const size = 10 + Buffer.byteLength(portal)
  const message = Buffer.allocUnsafe(size)
  let offset = 0
  offset = writeUint8(message, FrontendMessage.Execute, offset)
  offset = writeInt32(message, size - 1, offset)
  offset = writeCString(message, portal, offset)
  writeInt32(message, 0, offset)
  return message
}

export function getTypeScriptType(pgType: ObjectId) {
  switch (pgType) {
  case ObjectId.Bool:
    return 'boolean'
  case ObjectId.Int2:
  case ObjectId.Int4:
  case ObjectId.Float4:
  case ObjectId.Float8:
  case ObjectId.Oid:
  case ObjectId.Regproc:
    return 'number'
  case ObjectId.Int8:
    return 'bigint'
  case ObjectId.Char:
  case ObjectId.Varchar:
  case ObjectId.Text:
  case ObjectId.Bpchar:
  case ObjectId.Name:
  case ObjectId.Numeric:
    return 'string'
  case ObjectId.CharArray:
  case ObjectId.VarcharArray:
  case ObjectId.TextArray:
  case ObjectId.BpcharArray:
  case ObjectId.NameArray:
  case ObjectId.NumericArray:
    return 'string[]'
  case ObjectId.Timestamp:
  case ObjectId.Timestamptz:
    return 'Date'
  case ObjectId.Int2Array:
  case ObjectId.Int4Array:
  case ObjectId.Float4Array:
  case ObjectId.Float8Array:
    return 'number[]'
  case ObjectId.Int8Array:
    return 'bigint[]'
  case ObjectId.Bytea:
    return 'Buffer'
  case ObjectId.Json:
  case ObjectId.Jsonb:
    return 'any'
  default:
    console.warn(`[WARN] Tried mapping an unsupported type to TypeScript: ${ObjectId[pgType] || pgType}`)
    return 'any'
  }
}

function readUint8(buffer: Buffer, offset: number): number {
  return buffer[offset]
}

function writeUint8(buffer: Buffer, value: number, offset: number): number {
  buffer[offset++] = value
  return offset
}

function readInt16(buffer: Buffer, offset: number): number {
  const value = (buffer[offset] << 8) + buffer[offset + 1]
  return value | (value & 32768) * 0x1fffe
}

function writeInt16(buffer: Buffer, value: number, offset: number): number {
  buffer[offset++] = value >> 8
  buffer[offset++] = value
  return offset
}

function readUint16(buffer: Buffer, offset: number): number {
  return buffer[offset] * 256 + buffer[offset + 1]
}

function writeUint16(buffer: Buffer, value: number, offset: number): number {
  buffer[offset++] = value >> 8
  buffer[offset++] = value
  return offset
}

function readInt32(buffer: Buffer, offset: number): number {
  return (buffer[offset] << 24)
    + (buffer[++offset] << 16)
    + (buffer[++offset] << 8)
    + buffer[++offset]
}

function writeInt32(buffer: Buffer, value: number, offset: number): number {
  buffer[offset++] = value >> 24
  buffer[offset++] = value >> 16
  buffer[offset++] = value >> 8
  buffer[offset++] = value
  return offset
}

function readUint32(buffer: Buffer, offset: number): number {
  return buffer[offset] * 16_777_216
    + (buffer[++offset] << 16)
    + (buffer[++offset] << 8)
    + buffer[++offset]
}

function writeUint32(buffer: Buffer, value: number, offset: number): number {
  buffer[offset++] = value >> 24
  buffer[offset++] = value >> 16
  buffer[offset++] = value >> 8
  buffer[offset++] = value
  return offset
}

function readInt64(buffer: Buffer, offset: number): bigint {
  const value =
    (buffer[offset] << 24) +
    (buffer[++offset] << 16) +
    (buffer[++offset] << 8) +
    buffer[++offset]

  return (BigInt(value) << 32n) +
    BigInt(
      (buffer[++offset] * 16_777_216) +
      (buffer[++offset] << 16) +
      (buffer[++offset] << 8) +
      buffer[++offset]
    )
}

function writeInt64(buffer: Buffer, value: bigint, offset: number): number {
  let lo = Number(value & 4_294_967_295n)
  buffer[offset + 7] = lo
  lo = lo >> 8
  buffer[offset + 6] = lo
  lo = lo >> 8
  buffer[offset + 5] = lo
  lo = lo >> 8
  buffer[offset + 4] = lo

  let hi = Number(value >> 32n & 4_294_967_295n)
  buffer[offset + 3] = hi
  hi = hi >> 8
  buffer[offset + 2] = hi
  hi = hi >> 8
  buffer[offset + 1] = hi
  hi = hi >> 8
  buffer[offset] = hi

  return offset + 8
}

const float32Arr = new Float32Array(1)
const uint8Float32Arr = new Uint8Array(float32Arr.buffer)

const float64Arr = new Float64Array(1)
const uint8Float64Arr = new Uint8Array(float64Arr.buffer)

float32Arr[0] = -1
const bigEndian = uint8Float32Arr[3] === 0

const readFloat32 = bigEndian ? function readFloat32(buffer: Buffer, offset: number): number {
  uint8Float32Arr[0] = buffer[offset]
  uint8Float32Arr[1] = buffer[++offset]
  uint8Float32Arr[2] = buffer[++offset]
  uint8Float32Arr[3] = buffer[++offset]
  return float32Arr[0]
} : function readFloat32(buffer: Buffer, offset: number): number {
  uint8Float32Arr[3] = buffer[offset]
  uint8Float32Arr[2] = buffer[++offset]
  uint8Float32Arr[1] = buffer[++offset]
  uint8Float32Arr[0] = buffer[++offset]
  return float32Arr[0]
}

const writeFloat32 = bigEndian ? function writeFloat32(buffer: Buffer, value: number, offset: number): number {
  float32Arr[0] = value
  buffer[offset++] = uint8Float32Arr[0]
  buffer[offset++] = uint8Float32Arr[1]
  buffer[offset++] = uint8Float32Arr[2]
  buffer[offset++] = uint8Float32Arr[3]
  return offset
} : function writeFloat32(buffer: Buffer, value: number, offset: number): number {
  float32Arr[0] = value
  buffer[offset++] = uint8Float32Arr[3]
  buffer[offset++] = uint8Float32Arr[2]
  buffer[offset++] = uint8Float32Arr[1]
  buffer[offset++] = uint8Float32Arr[0]
  return offset
}

const readFloat64 = bigEndian ? function readFloat64(buffer: Buffer, offset: number): number {
  uint8Float64Arr[0] = buffer[offset]
  uint8Float64Arr[1] = buffer[++offset]
  uint8Float64Arr[2] = buffer[++offset]
  uint8Float64Arr[3] = buffer[++offset]
  uint8Float64Arr[4] = buffer[++offset]
  uint8Float64Arr[5] = buffer[++offset]
  uint8Float64Arr[6] = buffer[++offset]
  uint8Float64Arr[7] = buffer[++offset]
  return float64Arr[0]
} : function readFloat64(buffer: Buffer, offset: number): number {
  uint8Float64Arr[7] = buffer[offset]
  uint8Float64Arr[6] = buffer[++offset]
  uint8Float64Arr[5] = buffer[++offset]
  uint8Float64Arr[4] = buffer[++offset]
  uint8Float64Arr[3] = buffer[++offset]
  uint8Float64Arr[2] = buffer[++offset]
  uint8Float64Arr[1] = buffer[++offset]
  uint8Float64Arr[0] = buffer[++offset]
  return float64Arr[0]
}

const writeFloat64 = bigEndian ? function writeFloat64(buffer: Buffer, value: number, offset: number): number {
  float64Arr[0] = value
  buffer[offset++] = uint8Float64Arr[0]
  buffer[offset++] = uint8Float64Arr[1]
  buffer[offset++] = uint8Float64Arr[2]
  buffer[offset++] = uint8Float64Arr[3]
  buffer[offset++] = uint8Float64Arr[4]
  buffer[offset++] = uint8Float64Arr[5]
  buffer[offset++] = uint8Float64Arr[6]
  buffer[offset++] = uint8Float64Arr[7]
  return offset
} : function writeFloat64(buffer: Buffer, value: number, offset: number): number {
  float64Arr[0] = value
  buffer[offset++] = uint8Float64Arr[7]
  buffer[offset++] = uint8Float64Arr[6]
  buffer[offset++] = uint8Float64Arr[5]
  buffer[offset++] = uint8Float64Arr[4]
  buffer[offset++] = uint8Float64Arr[3]
  buffer[offset++] = uint8Float64Arr[2]
  buffer[offset++] = uint8Float64Arr[1]
  buffer[offset++] = uint8Float64Arr[0]
  return offset
}

const enum NumericSign {
  Plus             = 0x0000,
  Minus            = 0x4000,
  NaN              = 0xc000,
  Infinity         = 0xD000,
  NegativeInfinity = 0xF000,
}

// See https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c
function readNumeric(buffer: Buffer, offset: number): string {
  const sign = readUint16(buffer, offset + 4)
  if (sign === NumericSign.NaN) {
    return 'NaN'
  } else if (sign === NumericSign.Infinity) {
    return 'Infinity'
  } else if (sign === NumericSign.NegativeInfinity) {
    return '-Infinity'
  }

  const digitsCount = readUint16(buffer, offset)
  let weight = readInt16(buffer, offset + 2) // There are `weight + 1` digits before the decimal point.
  let result = sign === NumericSign.Minus ? '-' : ''
  let i = 0

  wholePart: {
    while (true) {
      if (i >= digitsCount) {
        weight = -1
      }
      if (weight < 0) {
        result += '0'
        break wholePart
      }

      const digit = readNumericDigit(buffer, i)
      i++
      weight--

      if (digit !== 0) {
        result += digit
        break
      }
    }

    while (weight >= 0 && i < digitsCount) {
      const digit = readNumericDigit(buffer, i)
      i++
      weight--
      result += ('' + (10_000 + digit)).substr(1)
    }

    while (weight >= 0) {
      result += '0000'
      weight--
    }
  }

  const decimalsCount = readUint16(buffer, offset + 6)
  if (decimalsCount > 0) {
    result += '.'

    const omittedZeros = -1 - weight
    if (omittedZeros > 0) {
      if (4 * omittedZeros > decimalsCount) {
        return result + '0'.repeat(decimalsCount)
      } else {
        result += '0'.repeat(4 * omittedZeros)
      }
    }

    while (-4 * weight <= decimalsCount) {
      if (i < digitsCount) {
        const digit = readNumericDigit(buffer, i)
        result += ('' + (10_000 + digit)).substr(1)
      } else {
        result += '0000'
      }
      i++
      weight--
    }

    const digit = i < digitsCount ? readNumericDigit(buffer, i) : 0
    result += ('' + (10_000 + digit)).substr(1, decimalsCount % 4)
  }

  return result
}

function readNumericDigit(buffer: Buffer, index: number) {
  return readUint16(buffer, 8 + 2 * index)
}

// NOTE Postgres 14 may support 'Infinity' and '-Infinity' in numeric fields.
function writeNumeric(buffer: Buffer, value: string, offset: number): number {
  if (value === 'NaN') {
    writeUint16(buffer, 0, offset) // Number of digits
    writeInt16(buffer, 0, offset + 2) // Weight
    writeUint16(buffer, NumericSign.NaN, offset + 4) // Sign
    return writeUint16(buffer, 0, offset + 6) // Number of decimals
  }

  let [wholePart, decimalPart = ''] = value.split('.')
  let sign = NumericSign.Plus
  if (wholePart[0] === '-') {
    sign = NumericSign.Minus
    wholePart = wholePart.substr(1)
  }
  const initialOffset = offset
  offset += 8

  let weight = -1
  if (wholePart.length > 0) {
    weight = Math.ceil(wholePart.length / 4 - 1)
    wholePart = '0'.repeat(4 - ((wholePart.length - 1) % 4 + 1)) + wholePart
    for (let i = 0; i < wholePart.length; i += 4) {
      offset = writeUint16(buffer, parseInt(wholePart.substr(i, 4), 10), offset)
    }
  }

  const decimalsCount = decimalPart.length
  if (decimalsCount > 0) {
    decimalPart += '0'.repeat(4 - ((decimalsCount - 1) % 4 + 1))
    for (let i = 0; i < decimalPart.length; i += 4) {
      offset = writeUint16(buffer, parseInt(decimalPart.substr(i, 4), 10), offset)
    }
  }

  writeUint16(buffer, (wholePart.length + decimalPart.length) / 4, initialOffset)
  writeInt16(buffer, weight, initialOffset + 2)
  writeUint16(buffer, sign, initialOffset + 4)
  writeUint16(buffer, decimalsCount, initialOffset + 6)

  return offset
}

const postgresEpoch = new Date('2000-01-01T00:00:00Z').getTime()

function readTimestamp(buffer: Buffer, offset: number): Date {
  const value = 4_294_967_296 * readInt32(buffer, offset) + readUint32(buffer, offset + 4)
  return new Date(Math.round(value / 1_000) + postgresEpoch)
}

function writeTimestamp(buffer: Buffer, value: Date, offset: number): number {
  const t = (value.getTime() - postgresEpoch) * 1_000
  offset = writeInt32(buffer, t / 4_294_967_296, offset)
  return writeUint32(buffer, t, offset)
}

function readUtf8String(buffer: Buffer, offset: number, size: number): string {
  return buffer.slice(offset, offset + size).toString('utf8')
}

function writeUtf8String(buffer: Buffer, value: string, offset: number): number {
  return offset + buffer.write(value, offset)
}

function readCString(buffer: Buffer, offset: number): string {
  let end = offset
  while (buffer[end] !== 0) ++end
  return buffer.slice(offset, end).toString('ascii')
}

function writeCString(buffer: Buffer, value: string, offset: number): number {
  offset += buffer.write(value, offset, 'ascii')
  buffer[offset++] = 0
  return offset
}

function readArray<T>(buffer: Buffer, readElem: (buffer: Buffer, offset: number, size: number) => T): T[] {
  let offset = 0
  /* const dimensions     = readInt32(buffer, offset)              ; */ offset += 4
  /* const hasNulls       = readInt32(buffer, offset) as 0 | 1     ; */ offset += 4
  /* const elemType       = readUint32(buffer, offset) as ObjectId ; */ offset += 4
  const dimensionSize  = readInt32(buffer, offset)              ; offset += 4
  /* const dimensionStart = readInt32(buffer, offset)              ; */ offset += 4

  const result: T[] = []
  for (let i = 0; i < dimensionSize; ++i) {
    const elemSize = readInt32(buffer, offset)          ; offset += 4
    const elem     = readElem(buffer, offset, elemSize) ; offset += elemSize
    result.push(elem)
  }
  return result
}

function writeArray<T>(buffer: Buffer, values: T[], offset: number, elemType: ObjectId, writeElem: (buffer: Buffer, value: T, offset: number) => number): number {
  offset = writeInt32(buffer, 1, offset)             // Number of dimensions
  offset = writeInt32(buffer, 0, offset)             // Has nulls?
  offset = writeUint32(buffer, elemType, offset)     // Element type
  offset = writeInt32(buffer, values.length, offset) // Size of first dimension
  offset = writeInt32(buffer, 1, offset)             // Offset (starting index) of first dimension
  for (const v of values) {
    const elemOffset = offset + 4
    offset = writeElem(buffer, v, elemOffset)
    writeInt32(buffer, offset - elemOffset, elemOffset - 4)
  }
  return offset
}

const enum FrontendMessage {
  Bind            =  66, // 'B'
  Close           =  67, // 'C'
  CopyData        = 100, // 'd'
  CopyDone        =  99, // 'c'
  Describe        =  68, // 'D'
  Execute         =  69, // 'E'
  Flush           =  72, // 'H'
  FunctionCall    =  70, // 'F'
  Parse           =  80, // 'P'
  PasswordMessage = 112, // 'p'
  Query           =  81, // 'Q'
  Sync            =  83, // 'S'
  Terminate       =  88, // 'X'
}

enum BackendMessage {
  Authentication           =  82, // 'R'
  BackendKeyData           =  75, // 'K'
  BindComplete             =  50, // '2'
  CloseComplete            =  51, // '3'
  CommandComplete          =  67, // 'C'
  CopyBothResponse         =  87, // 'W'
  CopyData                 = 100, // 'd'
  CopyDone                 =  99, // 'c'
  CopyInResponse           =  71, // 'G'
  CopyOutResponse          =  72, // 'H'
  DataRow                  =  68, // 'D'
  EmptyQueryResponse       =  73, // 'I'
  ErrorResponse            =  69, // 'E'
  FunctionCallResponse     =  86, // 'V'
  NegotiateProtocolVersion = 118, // 'v'
  NoData                   = 110, // 'n'
  NoticeResponse           =  78, // 'N'
  NotificationResponse     =  65, // 'A'
  ParameterDescription     = 116, // 't'
  ParameterStatus          =  83, // 'S'
  ParseComplete            =  49, // '1'
  PortalSuspended          = 115, // 's'
  ReadyForQuery            =  90, // 'Z'
  RowDescription           =  84, // 'T'
}

const enum ErrorResponseType {
  Code              =  67, // 'C'
  ColumnName        =  99, // 'c'
  ConstraintName    = 110, // 'n'
  DateTypeName      = 100, // 'd'
  Detail            =  68, // 'D'
  File              =  70, // 'F'
  Hint              =  72, // 'H'
  InternalPosition  = 112, // 'p'
  Line              =  76, // 'L'
  Message           =  77, // 'M'
  Position          =  80, // 'P'
  Routine           =  82, // 'R'
  SchemaName        = 115, // 's'
  Severity          =  86, // 'V'
  SeverityLocalized =  83, // 'S'
  Where             =  87, // 'W'
}

const enum DescribeOrCloseRequest {
  Portal            = 80, // 'P'
  PreparedStatement = 83, // 'S'
}

enum AuthenticationResponse {
  Ok                = 0,
  KerberosV5        = 2,
  CleartextPassword = 3,
  Md5Password       = 5,
  ScmCredential     = 6,
  Gss               = 7,
  GssContinue       = 8,
  Sspi              = 9,
  Sasl              = 10,
  SaslContinue      = 11,
  SaslFinal         = 12,
}

const enum WireFormat {
  Text   = 0,
  Binary = 1,
}

// const enum TransactionStatus {
//   Idle                     = 73, // 'I'
//   InTransactionBlock       = 84, // 'T'
//   InFailedTransactionBlock = 69, // 'E'
// }

export enum ObjectId {
  Aclitem               = 1033,
  AclitemArray          = 1034,
  Any                   = 2276,
  Anyarray              = 2277,
  Anycompatible         = 5077,
  Anycompatiblearray    = 5078,
  Anycompatiblenonarray = 5079,
  AnycompatibleRange    = 5080,
  Anyelement            = 2283,
  Anyenum               = 3500,
  Anynonarray           = 2776,
  AnyRange              = 3831,
  Bit                   = 1560,
  BitArray              = 1561,
  Bool                  = 16,
  BoolArray             = 1000,
  Box                   = 603,
  BoxArray              = 1020,
  Bpchar                = 1042,
  BpcharArray           = 1014,
  Bytea                 = 17,
  ByteaArray            = 1001,
  Char                  = 18,
  CharArray             = 1002,
  Cid                   = 29,
  CidArray              = 1012,
  Cidr                  = 650,
  CidrArray             = 651,
  Circle                = 718,
  CircleArray           = 719,
  Cstring               = 2275,
  CstringArray          = 1263,
  Date                  = 1082,
  DateArray             = 1182,
  DateRange             = 3912,
  DateRangeArray        = 3913,
  EventTrigger          = 3838,
  FdwHandler            = 3115,
  Float4                = 700,
  Float4Array           = 1021,
  Float8                = 701,
  Float8Array           = 1022,
  GtsVector             = 3642,
  GtsVectorArray        = 3644,
  IndexAmHandler        = 325,
  Inet                  = 869,
  InetArray             = 1041,
  Int2                  = 21,
  Int2Array             = 1005,
  Int2Vector            = 22,
  Int2VectorArray       = 1006,
  Int4                  = 23,
  Int4Array             = 1007,
  Int4Range             = 3904,
  Int4RangeArray        = 3905,
  Int8                  = 20,
  Int8Array             = 1016,
  Int8Range             = 3926,
  Int8RangeArray        = 3927,
  Internal              = 2281,
  Interval              = 1186,
  IntervalArray         = 1187,
  Json                  = 114,
  JsonArray             = 199,
  Jsonb                 = 3802,
  JsonbArray            = 3807,
  Jsonpath              = 4072,
  JsonpathArray         = 4073,
  LanguageHandler       = 2280,
  Line                  = 628,
  LineArray             = 629,
  Lseg                  = 601,
  LsegArray             = 1018,
  Macaddr               = 829,
  MacaddrArray          = 1040,
  Macaddr8              = 774,
  Macaddr8Array         = 775,
  Money                 = 790,
  MoneyArray            = 791,
  Name                  = 19,
  NameArray             = 1003,
  Numeric               = 1700,
  NumericArray          = 1231,
  NumRange              = 3906,
  NumRangeArray         = 3907,
  Oid                   = 26,
  OidArray              = 1028,
  OidVector             = 30,
  OidVectorArray        = 1013,
  Path                  = 602,
  PathArray             = 1019,
  PgDdlCommand          = 32,
  PgDependencies        = 3402,
  PgLsn                 = 3220,
  PgLsnArray            = 3221,
  PgMcvList             = 5017,
  PgNdistinct           = 3361,
  PgNodeTree            = 194,
  PgSnapshot            = 5038,
  PgSnapshotArray       = 5039,
  Point                 = 600,
  PointArray            = 1017,
  Polygon               = 604,
  PolygonArray          = 1027,
  Record                = 2249,
  RecordArray           = 2287,
  Refcursor             = 1790,
  RefcursorArray        = 2201,
  Regclass              = 2205,
  RegclassArray         = 2210,
  Regcollation          = 4191,
  RegcollationArray     = 4192,
  Regconfig             = 3734,
  RegconfigArray        = 3735,
  Regdictionary         = 3769,
  RegdictionaryArray    = 3770,
  Regnamespace          = 4089,
  RegnamespaceArray     = 4090,
  Regoper               = 2203,
  RegoperArray          = 2208,
  Regoperator           = 2204,
  RegoperatorArray      = 2209,
  Regproc               = 24,
  RegprocArray          = 1008,
  Regprocedure          = 2202,
  RegprocedureArray     = 2207,
  Regrole               = 4096,
  RegroleArray          = 4097,
  Regtype               = 2206,
  RegtypeArray          = 2211,
  TableAmHandler        = 269,
  Text                  = 25,
  TextArray             = 1009,
  Tid                   = 27,
  TidArray              = 1010,
  Time                  = 1083,
  TimeArray             = 1183,
  Timestamp             = 1114,
  TimestampArray        = 1115,
  Timestamptz           = 1184,
  TimestamptzArray      = 1185,
  Timetz                = 1266,
  TimetzArray           = 1270,
  Trigger               = 2279,
  TsmHandler            = 3310,
  Tsquery               = 3615,
  TsqueryArray          = 3645,
  TsRange               = 3908,
  TsRangeArray          = 3909,
  TstzRange             = 3910,
  TstzRangeArray        = 3911,
  TsVector              = 3614,
  TsVectorArray         = 3643,
  TxidSnapshot          = 2970,
  TxidSnapshotArray     = 2949,
  Unknown               = 705,
  Uuid                  = 2950,
  UuidArray             = 2951,
  Varbit                = 1562,
  VarbitArray           = 1563,
  Varchar               = 1043,
  VarcharArray          = 1015,
  Void                  = 2278,
  Xid                   = 28,
  XidArray              = 1011,
  Xid8                  = 5069,
  Xid8Array             = 271,
  Xml                   = 142,
  XmlArray              = 143,
}
