import { createHash } from 'crypto'
import { createConnection as createTcpConnection, Socket } from 'net'
import { connect as createTlsConnection, ConnectionOptions } from 'tls'

// References:
// - https://postgresql.org/docs/current/protocol.html
// - https://postgresql.org/docs/current/datatype.html
// - https://beta.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf
// - https://segmentfault.com/a/1190000017136059
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
  name = 'QueryCancelledError'
}

export class PostgresError extends Error {
  name = 'PostgresError'
  code!: ErrorCode
  message!: string
  columnName?: string
  constraintName?: string
  dateTypeName?: string
  detail?: string
  file?: string
  hint?: string
  internalPosition?: string
  line?: string
  position?: string
  routine?: string
  schemaName?: string
  severity?: string
  severityLocalized?: string
  where?: string

  constructor(data: Buffer) {
    super()

    const msgSize = 1 + readInt32(data, 1)
    let offset = 5
    while (offset < msgSize) {
      const type = readUint8(data, offset++)
      if (type === 0) continue

      const value = readCString(data, offset)
      offset += value.length + 1

      switch (type as ErrorResponseType) {
      case ErrorResponseType.Code:              this.code              = value as ErrorCode ; break
      case ErrorResponseType.Message:           this.message           = value              ; break
      case ErrorResponseType.ColumnName:        this.columnName        = value              ; break
      case ErrorResponseType.ConstraintName:    this.constraintName    = value              ; break
      case ErrorResponseType.DateTypeName:      this.dateTypeName      = value              ; break
      case ErrorResponseType.Detail:            this.detail            = value              ; break
      case ErrorResponseType.File:              this.file              = value              ; break
      case ErrorResponseType.Hint:              this.hint              = value              ; break
      case ErrorResponseType.InternalPosition:  this.internalPosition  = value              ; break
      case ErrorResponseType.Line:              this.line              = value              ; break
      case ErrorResponseType.Position:          this.position          = value              ; break
      case ErrorResponseType.Routine:           this.routine           = value              ; break
      case ErrorResponseType.SchemaName:        this.schemaName        = value              ; break
      case ErrorResponseType.Severity:          this.severity          = value              ; break
      case ErrorResponseType.SeverityLocalized: this.severityLocalized = value              ; break
      case ErrorResponseType.Where:             this.where             = value              ; break
      }
    }
  }
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

interface Connection extends Socket {
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

  const openConnections: Connection[] = []
  const availableConnections: Connection[] = []
  const waitingForConnection: ((conn: Connection) => void)[] = []

  for (let i = 0; i < options.minConnections; ++i)
    tryOpenConnection()

  function tryOpenConnection(retryDelay = 1) {
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
          setTimeout(() => tryOpenConnection(Math.min(1024, options.connectTimeout!, retryDelay * 2)), retryDelay)
      })
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
      resultPromise.finally(() => lendConnection(conn)).catch(() => {})
      resultPromise.cancel = resultPromise.cancel || (() => {})
      return resultPromise
    }

    if (openConnections.length < options.maxConnections!)
      tryOpenConnection()

    let cancelled = false

    const connPromise = new Promise<Connection>(resolve => waitingForConnection.push(resolve))

    const wrappingPromise = connPromise.then(conn => {
      if (cancelled)
        throw new QueryCancelledError('Query cancelled during connection acquisition phase.')

      const resultPromise = callback(conn) as CancellablePromise<T>
      resultPromise.finally(() => lendConnection(conn)).catch(() => {})
      wrappingPromise.cancel = resultPromise.cancel || (() => {})
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

      options.minConnections
        = options.maxConnections
        = waitingForConnection.length
        = availableConnections.length
        = openConnections.length
        = 0
    }
  }
}

function openConnection(options: PoolOptions): Promise<Connection> {
  return new Promise(async (resolve, reject) => {
    const conn = createTcpConnection(options.port, options.host) as Connection

    const timeoutId = setTimeout(
      () => handleStartupPhaseError(Error('Stopping connection attempt as it has been going on for too long.')),
      options.connectTimeout
    )

    let off: () => void

    if (options.ssl) {
      conn.once('connect', () => conn.write(sslRequestMessage))
      conn.once('data', data => {
        if (readUint8(data, 0) === 83) { // 'S'
          createTlsConnection({ socket: conn, ...options.ssl })
          conn.write(createStartupMessage(options.username, options.database))
          off = onConnectionData(conn, handleStartupPhase)
        } else {
          handleStartupPhaseError(Error('Postgres server does not support SSL.'))
        }
      })
    } else {
      conn.once('connect', () => {
        conn.write(createStartupMessage(options.username, options.database))
        off = onConnectionData(conn, handleStartupPhase)
      })
    }

    conn.once('error', handleStartupPhaseError)
    conn.once('close', () => handleStartupPhaseError(Error('Connection has been closed.')))

    function handleStartupPhaseError(err: Error) {
      reject(err)
      conn.destroy(err)
    }

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
          handleStartupPhaseError(Error(`Unsupported authentication response sent by server: "${AuthenticationResponse[authRes] || authRes}".`))
        }
      }
      else if (msgType === BackendMessage.ParameterStatus) {
        // const paramName = readCString(data, 5)
        // const paramValue = readCString(data, 5 + paramName.length + 1)
      }
      else if (msgType === BackendMessage.BackendKeyData) {
        conn.cancelKey = createCancelRequestMessage(readInt32(data, 5), readInt32(data, 9))
      }
      else if (msgType === BackendMessage.ReadyForQuery) {
        if (authOk) {
          clearTimeout(timeoutId)
          off()
          conn.preparedQueries = {}
          resolve(conn)
        } else {
          handleStartupPhaseError(Error('Authentication could not be completed.'))
        }
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        handleStartupPhaseError(new PostgresError(data))
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
        handleStartupPhaseError(Error(`The Postgres server does not support protocol versions greather than 3.${minorVersion}.${unrecognizedOptionsMsg}`))
      }
      else if (msgType === BackendMessage.NoticeResponse) {
        onNotice(data)
      }
      else {
        console.warn(`[WARN] Unexpected message type sent by server during startup phase: "${BackendMessage[msgType] || msgType}".`)
      }
    }
  })
}

function runSimpleQuery(conn: Connection, query: 'begin' | 'commit' | 'rollback' | `savepoint ${string}` | `rollback to ${string}` | `release ${string}`): Promise<void> {
  return new Promise((resolve, reject) => {
    const off = onConnectionData(conn, handleSimpleQueryExecution)
    conn.once('error', reject)
    conn.once('close', reject)
    conn.write(createSimpleQueryMessage(query))

    let commandCompleted = false

    function handleSimpleQueryExecution(data: Buffer): void {
      const msgType = readUint8(data, 0) as BackendMessage
      if (msgType === BackendMessage.CommandComplete) {
        commandCompleted = true
      }
      else if (msgType === BackendMessage.ReadyForQuery) {
        off()
        conn.off('error', reject)
        conn.off('close', reject)
        if (commandCompleted)
          resolve()
        else
          reject(Error('Failed to execute simple query.'))
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        off()
        conn.off('error', reject)
        conn.off('close', reject)
        reject(new PostgresError(data))
      }
      else if (msgType === BackendMessage.NoticeResponse) {
        onNotice(data)
      }
      else {
        console.warn(`[WARN] Unexpected message received during simple query execution phase: ${BackendMessage[msgType] || msgType}.`)
      }
    }
  })
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
      if (cancelled && !(err instanceof QueryCancelledError)) {
        try { await cancelledPromise! } catch {}
        throw new QueryCancelledError(err.message)
      }
      throw err
    } finally {
      clearTimeout(timeoutId)
    }
  })() as CancellablePromise<QueryResult<R>>

  resultPromise.cancel = cancel
  return resultPromise
}

function prepareQuery(conn: Connection, queryId: string, querySql: string): Promise<QueryMetadata> {
  const { preparedQueries } = conn
  if (queryId && preparedQueries[queryId])
    return Promise.resolve(preparedQueries[queryId])

  return new Promise((resolve, reject) => {
    let parseCompleted = false
    let paramTypesFetched = false
    let rowMetadataFetched = false

    const paramTypes: ObjectId[] = []
    const rowMetadata: ColumnMetadata[] = []

    const off = onConnectionData(conn, handleQueryPreparation)
    conn.once('error', reject)
    conn.once('close', reject)
    conn.write(Buffer.concat([
      createParseMessage(querySql, queryId, []),
      createDescribeMessage(DescribeType.PreparedStatement, queryId),
      syncMessage
    ]))

    function handleQueryPreparation(data: Buffer): void {
      const msgType = readUint8(data, 0) as BackendMessage
      if (msgType === BackendMessage.ParseComplete) {
        parseCompleted = true
      }
      else if (msgType === BackendMessage.ParameterDescription) {
        const paramCount = readInt16(data, 5)
        let offset = 7
        for (let i = 0; i < paramCount; ++i) {
          const paramType = readInt32(data, offset)
          offset += 4
          paramTypes!.push(paramType)
        }
        paramTypesFetched = true
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
      // "NoData" is expected for SQL queries without return value, e.g. DDL statements.
      else if (msgType === BackendMessage.ReadyForQuery || msgType === BackendMessage.NoData) {
        off()
        conn.off('error', reject)
        conn.off('close', reject)
        if (parseCompleted && (paramTypesFetched && (rowMetadataFetched || msgType === BackendMessage.NoData))) {
          const queryMetadata = { paramTypes, rowMetadata }
          if (queryId)
            preparedQueries[queryId] = queryMetadata

          resolve(queryMetadata)
        } else {
          reject(Error('Failed to parse query.'))
        }
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        off()
        conn.off('error', reject)
        conn.off('close', reject)
        reject(new PostgresError(data))
      }
      else if (msgType === BackendMessage.NoticeResponse) {
        onNotice(data)
      }
      else {
        console.warn(`[WARN] Unexpected message received during query preparation phase: ${BackendMessage[msgType] || msgType}`)
      }
    }
  })
}

const commandsWithRowsAffected = ['INSERT', 'DELETE', 'UPDATE', 'SELECT', 'MOVE', 'FETCH', 'COPY']

function runExtendedQuery<R extends Row>(conn: Connection, query: Required<Query>): Promise<QueryResult<R>> {
  return new Promise((resolve, reject) => {
    let preparedQuery: QueryMetadata | undefined
    const { preparedQueries } = conn
    if (query.id && preparedQueries[query.id])
      preparedQuery = preparedQueries[query.id]

    let parseCompleted = preparedQuery ? true : false
    let bindingCompleted = false
    let commandCompleted = false

    const { rowMetadata } = query.metadata
    const rows: R[] = []
    let rowsAffected = 0

    const off = onConnectionData(conn, handleQueryExecution)
    conn.once('error', reject)
    conn.once('close', reject)
    conn.write(Buffer.concat([
      preparedQuery ? Buffer.of() : createParseMessage(query.sql, query.id, []),
      createBindMessage(query.id, query.params, query.metadata.paramTypes, ''),
      executeUnnamedPortalMessage,
      syncMessage
    ]))

    function handleQueryExecution(data: Buffer): void {
      const msgType = readUint8(data, 0) as BackendMessage
      if (msgType === BackendMessage.DataRow) {
        // const paramCount = readInt16(data, 5)
        const row: Row = {}
        let offset = 7
        for (let i = 0; i < rowMetadata.length; ++i) {
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
      else if (msgType === BackendMessage.ParseComplete) {
        parseCompleted = true
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
        off()
        conn.off('error', reject)
        conn.off('close', reject)
        if (parseCompleted && bindingCompleted && commandCompleted) {
          if (query.id)
            preparedQueries[query.id] = query.metadata

          resolve({ rows, rowsAffected })
        } else {
          reject(Error('Failed to execute prepared query.'))
        }
      }
      else if (msgType === BackendMessage.ErrorResponse) {
        off()
        conn.off('error', reject)
        conn.off('close', reject)
        reject(new PostgresError(data))
      }
      else if (msgType === BackendMessage.NoticeResponse) {
        onNotice(data)
      }
      else {
        console.warn(`[WARN] Unexpected message received during prepared query execution phase: ${BackendMessage[msgType] || msgType}.`)
      }
    }
  })
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

function onConnectionData(conn: Connection, callback: (data: Buffer) => void) {
  conn.on('data', dataHandler)

  let leftover: Buffer | undefined

  function dataHandler(data: Buffer) {
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

    callback(data)

    if (data.byteLength > msgSize)
      dataHandler(data.slice(msgSize))
  }

  return () => conn.off('data', dataHandler)
}

function onNotice(data: Buffer) {
  console.log('Postgres notice:', new PostgresError(data))
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
  if (differentNames)
    // 10 = 9 ("database" and null terminator) + 1 (database null terminator)
    size += 10 + Buffer.byteLength(database)

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

function createCancelRequestMessage(processId: number, secretKey: number): Buffer {
  const size = 16
  const message = Buffer.allocUnsafe(size)
  writeInt32(message, size, 0)
  writeInt32(message, 80877102, 4) // Cancel request code
  writeInt32(message, processId, 8)
  writeInt32(message, secretKey, 12)
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

  for (const t of paramTypes)
    offset = writeInt32(message, t, offset)

  return message
}

function createDescribeMessage(type: DescribeType, id: string): Buffer {
  // 7 = 1 (message type) + 4 (message size) + 1 (describe message type) + 1 (queryId null terminator)
  const size = 7 + Buffer.byteLength(id)
  const message = Buffer.allocUnsafe(size)
  writeUint8(message, FrontendMessage.Describe, 0)
  writeInt32(message, size - 1, 1)
  writeUint8(message, type, 5)
  writeCString(message, id, 6)
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
  let message = Buffer.allocUnsafe(4_096)
  let offset = 0

  offset = writeUint8(message, FrontendMessage.Bind, offset)
  offset += 4 // Message size to be placed here.
  offset = writeCString(message, portal, offset)
  offset = writeCString(message, queryId, offset)
  offset = writeInt16(message, 1, offset)
  offset = writeInt16(message, WireFormat.Binary, offset)
  offset = writeInt16(message, params.length, offset)

  for (let i = 0; i < params.length; ++i) {
    const v = params[i]
    const priorOffset = offset

    if (v == null) {
      offset = writeInt32(message, -1, offset)
    } else {
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
      case ObjectId.Bytea:        offset = writeBuffer(message, v as Buffer, offset)                                     ; break
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
      const messageSize = offset - (priorOffset + 4)
      writeInt32(message, messageSize, priorOffset)
    }

    // 4 = bytes written after loop
    if (4 + offset >= message.length) {
      const priorMessage = message
      let newSize = priorMessage.length * 2
      while (4 + offset >= newSize) newSize *= 2
      message = Buffer.allocUnsafe(newSize)
      priorMessage.copy(message, 0, 0, priorOffset)
      offset = priorOffset
      i--
      continue
    }
  }

  offset = writeInt16(message, 1, offset)
  offset = writeInt16(message, WireFormat.Binary, offset)

  writeInt32(message, offset - 1, 1)
  return message.slice(0, offset)
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
  if (sign === NumericSign.NaN)
    return 'NaN'
  else if (sign === NumericSign.Infinity)
    return 'Infinity'
  else if (sign === NumericSign.NegativeInfinity)
    return '-Infinity'

  const digitsInBuffer = readUint16(buffer, offset) // Number of base-10000 digits in the buffer
  const weight = readInt16(buffer, offset + 2)
  const wholesCount = weight + 1 // There are `weight + 1` base-10000 digits before the decimal point in the decoded number
  const wholesInBuffer = Math.max(0, Math.min(wholesCount, digitsInBuffer))
  let result = sign === NumericSign.Minus ? '-' : ''

  if (digitsInBuffer === 0 || wholesCount <= 0) {
    result += '0'
  } else {
    if (wholesInBuffer > 0)
      result += readNumericDigit(buffer, 0)

    for (let i = 1; i < wholesInBuffer; ++i)
      result += ('' + readNumericDigit(buffer, i)).padStart(4, '0')

    const omittedZeros = wholesCount - wholesInBuffer
    if (omittedZeros > 0)
      result += '0'.repeat(4 * omittedZeros)
  }

  const decimalsCount = readUint16(buffer, offset + 6) // Number of base-10 decimals in the decoded number
  if (decimalsCount > 0) {
    result += '.'

    let decimals = ''

    const omittedZeros = wholesCount < 0 ? -1 * wholesCount : 0
    decimals += '0'.repeat(4 * omittedZeros)

    for (let i = wholesInBuffer; i < digitsInBuffer; ++i)
      decimals += ('' + readNumericDigit(buffer, i)).padStart(4, '0')

    result += decimals.length < decimalsCount
      ? decimals.padEnd(decimalsCount, '0')
      : decimals.length > decimalsCount
      ? decimals.substr(0, decimalsCount)
      : decimals
  }

  return result
}

function readNumericDigit(buffer: Buffer, idx: number) {
  return readUint16(buffer, 8 + 2 * idx)
}

// NOTE Postgres 14 may support 'Infinity' and '-Infinity' in numeric fields.
function writeNumeric(buffer: Buffer, value: string, offset: number): number {
  if (value === 'NaN') {
    writeUint16(buffer, 0, offset)
    writeInt16(buffer, 0, offset + 2)
    writeUint16(buffer, NumericSign.NaN, offset + 4)
    return writeUint16(buffer, 0, offset + 6)
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
    weight = Math.ceil(wholePart.length / 4) - 1
    wholePart = '0'.repeat(4 - ((wholePart.length - 1) % 4 + 1)) + wholePart
    for (let i = 0; i < wholePart.length; i += 4)
      offset = writeUint16(buffer, parseInt(wholePart.substr(i, 4), 10), offset)
  }

  const decimalsCount = decimalPart.length
  if (decimalsCount > 0) {
    decimalPart += '0'.repeat(4 - ((decimalsCount - 1) % 4 + 1))
    for (let i = 0; i < decimalPart.length; i += 4)
      offset = writeUint16(buffer, parseInt(decimalPart.substr(i, 4), 10), offset)
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

function writeBuffer(buffer: Buffer, value: Buffer, offset: number): number {
  value.copy(buffer, offset)
  return offset + value.length
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

const enum DescribeType {
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

const enum ErrorResponseType {
  Code              =  67, // 'C'
  Message           =  77, // 'M'
  ColumnName        =  99, // 'c'
  ConstraintName    = 110, // 'n'
  DateTypeName      = 100, // 'd'
  Detail            =  68, // 'D'
  File              =  70, // 'F'
  Hint              =  72, // 'H'
  InternalPosition  = 112, // 'p'
  Line              =  76, // 'L'
  Position          =  80, // 'P'
  Routine           =  82, // 'R'
  SchemaName        = 115, // 's'
  Severity          =  86, // 'V'
  SeverityLocalized =  83, // 'S'
  Where             =  87, // 'W'
}

// https://postgresql.org/docs/current/errcodes-appendix.html
const enum ErrorCode {
  successful_completion                                = '00000',
  warning                                              = '01000',
  dynamic_result_sets_returned                         = '0100C',
  implicit_zero_bit_padding                            = '01008',
  null_value_eliminated_in_set_function                = '01003',
  privilege_not_granted                                = '01007',
  privilege_not_revoked                                = '01006',
  string_data_right_truncation                         = '01004',
  deprecated_feature                                   = '01P01',
  no_data                                              = '02000',
  no_additional_dynamic_result_sets_returned           = '02001',
  sql_statement_not_yet_complete                       = '03000',
  connection_exception                                 = '08000',
  connection_does_not_exist                            = '08003',
  connection_failure                                   = '08006',
  sqlclient_unable_to_establish_sqlconnection          = '08001',
  sqlserver_rejected_establishment_of_sqlconnection    = '08004',
  transaction_resolution_unknown                       = '08007',
  protocol_violation                                   = '08P01',
  triggered_action_exception                           = '09000',
  feature_not_supported                                = '0A000',
  invalid_transaction_initiation                       = '0B000',
  locator_exception                                    = '0F000',
  invalid_locator_specification                        = '0F001',
  invalid_grantor                                      = '0L000',
  invalid_grant_operation                              = '0LP01',
  invalid_role_specification                           = '0P000',
  diagnostics_exception                                = '0Z000',
  stacked_diagnostics_accessed_without_active_handler  = '0Z002',
  case_not_found                                       = '20000',
  cardinality_violation                                = '21000',
  data_exception                                       = '22000',
  array_subscript_error                                = '2202E',
  character_not_in_repertoire                          = '22021',
  datetime_field_overflow                              = '22008',
  division_by_zero                                     = '22012',
  error_in_assignment                                  = '22005',
  escape_character_conflict                            = '2200B',
  indicator_overflow                                   = '22022',
  interval_field_overflow                              = '22015',
  invalid_argument_for_logarithm                       = '2201E',
  invalid_argument_for_ntile_function                  = '22014',
  invalid_argument_for_nth_value_function              = '22016',
  invalid_argument_for_power_function                  = '2201F',
  invalid_argument_for_width_bucket_function           = '2201G',
  invalid_character_value_for_cast                     = '22018',
  invalid_datetime_format                              = '22007',
  invalid_escape_character                             = '22019',
  invalid_escape_octet                                 = '2200D',
  invalid_escape_sequence                              = '22025',
  nonstandard_use_of_escape_character                  = '22P06',
  invalid_indicator_parameter_value                    = '22010',
  invalid_parameter_value                              = '22023',
  invalid_preceding_or_following_size                  = '22013',
  invalid_regular_expression                           = '2201B',
  invalid_row_count_in_limit_clause                    = '2201W',
  invalid_row_count_in_result_offset_clause            = '2201X',
  invalid_tablesample_argument                         = '2202H',
  invalid_tablesample_repeat                           = '2202G',
  invalid_time_zone_displacement_value                 = '22009',
  invalid_use_of_escape_character                      = '2200C',
  most_specific_type_mismatch                          = '2200G',
  null_value_not_allowed                               = '22004',
  null_value_no_indicator_parameter                    = '22002',
  numeric_value_out_of_range                           = '22003',
  sequence_generator_limit_exceeded                    = '2200H',
  string_data_length_mismatch                          = '22026',
  substring_error                                      = '22011',
  trim_error                                           = '22027',
  unterminated_c_string                                = '22024',
  zero_length_character_string                         = '2200F',
  floating_point_exception                             = '22P01',
  invalid_text_representation                          = '22P02',
  invalid_binary_representation                        = '22P03',
  bad_copy_file_format                                 = '22P04',
  untranslatable_character                             = '22P05',
  not_an_xml_document                                  = '2200L',
  invalid_xml_document                                 = '2200M',
  invalid_xml_content                                  = '2200N',
  invalid_xml_comment                                  = '2200S',
  invalid_xml_processing_instruction                   = '2200T',
  duplicate_json_object_key_value                      = '22030',
  invalid_argument_for_sql_json_datetime_function      = '22031',
  invalid_json_text                                    = '22032',
  invalid_sql_json_subscript                           = '22033',
  more_than_one_sql_json_item                          = '22034',
  no_sql_json_item                                     = '22035',
  non_numeric_sql_json_item                            = '22036',
  non_unique_keys_in_a_json_object                     = '22037',
  singleton_sql_json_item_required                     = '22038',
  sql_json_array_not_found                             = '22039',
  sql_json_member_not_found                            = '2203A',
  sql_json_number_not_found                            = '2203B',
  sql_json_object_not_found                            = '2203C',
  too_many_json_array_elements                         = '2203D',
  too_many_json_object_members                         = '2203E',
  sql_json_scalar_required                             = '2203F',
  integrity_constraint_violation                       = '23000',
  restrict_violation                                   = '23001',
  not_null_violation                                   = '23502',
  foreign_key_violation                                = '23503',
  unique_violation                                     = '23505',
  check_violation                                      = '23514',
  exclusion_violation                                  = '23P01',
  invalid_cursor_state                                 = '24000',
  invalid_transaction_state                            = '25000',
  active_sql_transaction                               = '25001',
  branch_transaction_already_active                    = '25002',
  held_cursor_requires_same_isolation_level            = '25008',
  inappropriate_access_mode_for_branch_transaction     = '25003',
  inappropriate_isolation_level_for_branch_transaction = '25004',
  no_active_sql_transaction_for_branch_transaction     = '25005',
  read_only_sql_transaction                            = '25006',
  schema_and_data_statement_mixing_not_supported       = '25007',
  no_active_sql_transaction                            = '25P01',
  in_failed_sql_transaction                            = '25P02',
  idle_in_transaction_session_timeout                  = '25P03',
  invalid_sql_statement_name                           = '26000',
  triggered_data_change_violation                      = '27000',
  invalid_authorization_specification                  = '28000',
  invalid_password                                     = '28P01',
  dependent_privilege_descriptors_still_exist          = '2B000',
  dependent_objects_still_exist                        = '2BP01',
  invalid_transaction_termination                      = '2D000',
  sql_routine_exception                                = '2F000',
  function_executed_no_return_statement                = '2F005',
  modifying_sql_data_not_permitted                     = '2F002',
  prohibited_sql_statement_attempted                   = '2F003',
  reading_sql_data_not_permitted                       = '2F004',
  invalid_cursor_name                                  = '34000',
  external_routine_exception                           = '38000',
  containing_sql_not_permitted                         = '38001',
  external_routine_invocation_exception                = '39000',
  invalid_sqlstate_returned                            = '39001',
  trigger_protocol_violated                            = '39P01',
  srf_protocol_violated                                = '39P02',
  event_trigger_protocol_violated                      = '39P03',
  savepoint_exception                                  = '3B000',
  invalid_savepoint_specification                      = '3B001',
  invalid_catalog_name                                 = '3D000',
  invalid_schema_name                                  = '3F000',
  transaction_rollback                                 = '40000',
  transaction_integrity_constraint_violation           = '40002',
  serialization_failure                                = '40001',
  statement_completion_unknown                         = '40003',
  deadlock_detected                                    = '40P01',
  syntax_error_or_access_rule_violation                = '42000',
  syntax_error                                         = '42601',
  insufficient_privilege                               = '42501',
  cannot_coerce                                        = '42846',
  grouping_error                                       = '42803',
  windowing_error                                      = '42P20',
  invalid_recursion                                    = '42P19',
  invalid_foreign_key                                  = '42830',
  invalid_name                                         = '42602',
  name_too_long                                        = '42622',
  reserved_name                                        = '42939',
  datatype_mismatch                                    = '42804',
  indeterminate_datatype                               = '42P18',
  collation_mismatch                                   = '42P21',
  indeterminate_collation                              = '42P22',
  wrong_object_type                                    = '42809',
  generated_always                                     = '428C9',
  undefined_column                                     = '42703',
  undefined_function                                   = '42883',
  undefined_table                                      = '42P01',
  undefined_parameter                                  = '42P02',
  undefined_object                                     = '42704',
  duplicate_column                                     = '42701',
  duplicate_cursor                                     = '42P03',
  duplicate_database                                   = '42P04',
  duplicate_function                                   = '42723',
  duplicate_prepared_statement                         = '42P05',
  duplicate_schema                                     = '42P06',
  duplicate_table                                      = '42P07',
  duplicate_alias                                      = '42712',
  duplicate_object                                     = '42710',
  ambiguous_column                                     = '42702',
  ambiguous_function                                   = '42725',
  ambiguous_parameter                                  = '42P08',
  ambiguous_alias                                      = '42P09',
  invalid_column_reference                             = '42P10',
  invalid_column_definition                            = '42611',
  invalid_cursor_definition                            = '42P11',
  invalid_database_definition                          = '42P12',
  invalid_function_definition                          = '42P13',
  invalid_prepared_statement_definition                = '42P14',
  invalid_schema_definition                            = '42P15',
  invalid_table_definition                             = '42P16',
  invalid_object_definition                            = '42P17',
  with_check_option_violation                          = '44000',
  insufficient_resources                               = '53000',
  disk_full                                            = '53100',
  out_of_memory                                        = '53200',
  too_many_connections                                 = '53300',
  configuration_limit_exceeded                         = '53400',
  program_limit_exceeded                               = '54000',
  statement_too_complex                                = '54001',
  too_many_columns                                     = '54011',
  too_many_arguments                                   = '54023',
  object_not_in_prerequisite_state                     = '55000',
  object_in_use                                        = '55006',
  cant_change_runtime_param                            = '55P02',
  lock_not_available                                   = '55P03',
  unsafe_new_enum_value_usage                          = '55P04',
  operator_intervention                                = '57000',
  query_canceled                                       = '57014',
  admin_shutdown                                       = '57P01',
  crash_shutdown                                       = '57P02',
  cannot_connect_now                                   = '57P03',
  database_dropped                                     = '57P04',
  system_error                                         = '58000',
  io_error                                             = '58030',
  undefined_file                                       = '58P01',
  duplicate_file                                       = '58P02',
  snapshot_too_old                                     = '72000',
  config_file_error                                    = 'F0000',
  lock_file_exists                                     = 'F0001',
  fdw_error                                            = 'HV000',
  fdw_column_name_not_found                            = 'HV005',
  fdw_dynamic_parameter_value_needed                   = 'HV002',
  fdw_function_sequence_error                          = 'HV010',
  fdw_inconsistent_descriptor_information              = 'HV021',
  fdw_invalid_attribute_value                          = 'HV024',
  fdw_invalid_column_name                              = 'HV007',
  fdw_invalid_column_number                            = 'HV008',
  fdw_invalid_data_type                                = 'HV004',
  fdw_invalid_data_type_descriptors                    = 'HV006',
  fdw_invalid_descriptor_field_identifier              = 'HV091',
  fdw_invalid_handle                                   = 'HV00B',
  fdw_invalid_option_index                             = 'HV00C',
  fdw_invalid_option_name                              = 'HV00D',
  fdw_invalid_string_length_or_buffer_length           = 'HV090',
  fdw_invalid_string_format                            = 'HV00A',
  fdw_invalid_use_of_null_pointer                      = 'HV009',
  fdw_too_many_handles                                 = 'HV014',
  fdw_out_of_memory                                    = 'HV001',
  fdw_no_schemas                                       = 'HV00P',
  fdw_option_name_not_found                            = 'HV00J',
  fdw_reply_handle                                     = 'HV00K',
  fdw_schema_not_found                                 = 'HV00Q',
  fdw_table_not_found                                  = 'HV00R',
  fdw_unable_to_create_execution                       = 'HV00L',
  fdw_unable_to_create_reply                           = 'HV00M',
  fdw_unable_to_establish_connection                   = 'HV00N',
  plpgsql_error                                        = 'P0000',
  raise_exception                                      = 'P0001',
  no_data_found                                        = 'P0002',
  too_many_rows                                        = 'P0003',
  assert_failure                                       = 'P0004',
  internal_error                                       = 'XX000',
  data_corrupted                                       = 'XX001',
  index_corrupted                                      = 'XX002',
}

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
