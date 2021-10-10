import { randomBytes } from 'crypto'
import type { Socket } from 'net'
import { createCancelRequestMessage, createCleartextPasswordMessage, createMd5PasswordMessage, createSaslContinueResponseMessage, createSaslInitialResponseMessage } from './frontend'
import { ObjectId } from './ObjectId'
import { PostgresError, PostgresErrorOrNotice, PostgresNotice } from './PostgresError'
import { readArray, readCString, readFloat32, readFloat64, readInt16, readInt32, readInt64, readNumeric, readTimestamp, readUint32, readUint8, readUtf8String } from './serialization'

export interface BackendSuccess<D> {
   data: D
   notices: PostgresNotice[]
   unexpected: UnexpectedBackendMessage[]
}

export interface BackendFailure {
   error: any
   notices: PostgresNotice[]
   unexpected: UnexpectedBackendMessage[]
}

export class UnexpectedBackendMessage {
   readonly time = Date.now()

   constructor(
      readonly type: BackendMessage,
      readonly data: Buffer
   ) {}
}

export function isSslSuppported(data: Buffer) {
   return readUint8(data, 0) === 83 // 'S'
}

export function handleStartupPhase(conn: Socket, username: string, password: string) {
   let authOk = false
   let cancelKey: Buffer
   let nonce = ''
   let serverSignature = ''

   return handleBackendMessage<Buffer>(conn, (messageType, data) => {
      if (messageType === BackendMessage.Authentication) {
         const authRes = readInt32(data, 5) as AuthenticationResponse
         if (authRes === AuthenticationResponse.Sasl) {
            nonce = randomBytes(16).toString('base64')
            conn.write(createSaslInitialResponseMessage(username, nonce))
            return [MessageHandlerResponseType.DONE_PARTIAL]
         }
         else if (authRes === AuthenticationResponse.SaslContinue) {
            const res = createSaslContinueResponseMessage(username, password, nonce, data.toString('utf8', 9))
            conn.write(res[0])
            serverSignature = res[1]
            return [MessageHandlerResponseType.DONE_PARTIAL]
         }
         else if (authRes === AuthenticationResponse.SaslFinal) {
            const serverResponse = data.toString('utf8', 11, data.indexOf('\0', 11) - 1)
            return serverResponse === serverSignature
               ? [MessageHandlerResponseType.DONE_PARTIAL]
               : [MessageHandlerResponseType.FAIL, Error(`Authentication failed for user "${username}": wrong SASL signature received from server.`)]
         }
         else if (authRes === AuthenticationResponse.Md5Password) {
            const salt = data.slice(9)
            conn.write(createMd5PasswordMessage(username, password, salt))
            return [MessageHandlerResponseType.DONE_PARTIAL]
         }
         else if (authRes === AuthenticationResponse.Ok) {
            authOk = true
            return [MessageHandlerResponseType.DONE_PARTIAL]
         }
         else if (authRes === AuthenticationResponse.CleartextPassword) {
            conn.write(createCleartextPasswordMessage(password))
            return [MessageHandlerResponseType.DONE_PARTIAL]
         }
         else
            return [MessageHandlerResponseType.FAIL, Error(`Unsupported authentication response sent by server: "${AuthenticationResponse[authRes] || authRes}".`)]
      }
      else if (messageType === BackendMessage.ParameterStatus)
         // const paramName = readCString(data, 5)
         // const paramValue = readCString(data, 5 + paramName.length + 1)
         return [MessageHandlerResponseType.DONE_PARTIAL]
      else if (messageType === BackendMessage.BackendKeyData) {
         cancelKey = createCancelRequestMessage(readInt32(data, 5), readInt32(data, 9))
         return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.ReadyForQuery)
         return authOk
            ? [MessageHandlerResponseType.DONE_FINAL, cancelKey]
            : [MessageHandlerResponseType.FAIL, Error('Authentication could not be completed.')]
      else if (messageType === BackendMessage.NegotiateProtocolVersion) {
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
         return [MessageHandlerResponseType.FAIL, Error(`The Postgres server does not support protocol versions greather than 3.${minorVersion}.${unrecognizedOptionsMsg}`)]
      }
      else
         return [MessageHandlerResponseType.UNPROCESSED]
   })
}

export function handleSimpleQueryExecution(conn: Socket) {
   let commandCompleted = false

   return handleBackendMessage<void>(conn, messageType => {
      if (messageType === BackendMessage.CommandComplete) {
         commandCompleted = true
         return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.ReadyForQuery)
         return commandCompleted
            ? [MessageHandlerResponseType.DONE_FINAL, undefined]
            : [MessageHandlerResponseType.FAIL, Error('Failed to execute simple query: command did not complete.')]
      else
         return [MessageHandlerResponseType.UNPROCESSED]
   })
}

export interface QueryMetadata {
   paramTypes: ObjectId[]
   rowMetadata: ColumnMetadata[]
}

export function handleQueryPreparation(conn: Socket) {
   let parseCompleted = false
   let paramTypesFetched = false
   let rowMetadataFetched = false

   const paramTypes: ObjectId[] = []
   const rowMetadata: ColumnMetadata[] = []

   return handleBackendMessage<QueryMetadata>(conn, (messageType, data) => {
      if (messageType === BackendMessage.ParseComplete) {
         parseCompleted = true
         return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.ParameterDescription) {
         const paramCount = readInt16(data, 5)
         let offset = 7
         for (let i = 0; i < paramCount; ++i) {
            const paramType = readInt32(data, offset)
            offset += 4
            paramTypes!.push(paramType)
         }
         paramTypesFetched = true
         return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.RowDescription) {
         const colCount = readInt16(data, 5)
         let offset = 7
         for (let i = 0; i < colCount; ++i) {
            const name            = readCString(data, offset)            ; offset += name.length + 1
            const tableId         = readInt32(data, offset) || undefined ; offset += 4
            const positionInTable = readInt16(data, offset) || undefined ; offset += 2
            const type            = readInt32(data, offset)              ; offset += 4
            /* const typeSize        = readInt16(data, offset)               ; */ offset += 2
            /* const typeModifier    = readInt32(data, offset)               ; */ offset += 4
            /* const format          = readInt16(data, offset) as WireFormat ; */ offset += 2
            rowMetadata.push({ name, type, tableId, positionInTable })
         }
         rowMetadataFetched = true
         return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      // "NoData" is expected for SQL queries without return value, e.g. DDL statements.
      else if (messageType === BackendMessage.ReadyForQuery || messageType === BackendMessage.NoData) {
         return parseCompleted && (paramTypesFetched && (rowMetadataFetched || messageType === BackendMessage.NoData))
            ? [MessageHandlerResponseType.DONE_FINAL, { paramTypes, rowMetadata }]
            : [MessageHandlerResponseType.FAIL, Error('Failed to parse query: operation did not complete.')]
      }
      else
         return [MessageHandlerResponseType.UNPROCESSED]
   })
}

const commandsWithRowsAffected = ['INSERT', 'DELETE', 'UPDATE', 'SELECT', 'MOVE', 'FETCH', 'COPY']

export interface QueryResult<R extends Row> {
   rows: R[]
   rowsAffected: number
}

export type Row = {
   [columnName: string]: ColumnValue
}

export type ColumnValue = any // undefined | null | boolean | number | number[] | bigint | bigint[] | string | string[] | Date | Buffer

export function handleQueryExecution<R extends Row>(conn: Socket, rowMetadata: ColumnMetadata[], skipParse: boolean) {
   let parseCompleted = skipParse
   let bindingCompleted = false
   let commandCompleted = false

   const rows: R[] = []
   let rowsAffected = 0

   return handleBackendMessage<QueryResult<R>>(conn, (messageType, data) => {
      if (messageType === BackendMessage.DataRow) {
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
         return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.ParseComplete) {
        parseCompleted = true
        return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.BindComplete) {
        bindingCompleted = true
        return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.CommandComplete) {
         const commandTagParts = readCString(data, 5).split(' ')
         const commandTag = commandTagParts[0]
         if (commandsWithRowsAffected.indexOf(commandTag) > -1)
            rowsAffected = parseInt(commandTagParts[commandTagParts.length - 1], 10)

         commandCompleted = true
         return [MessageHandlerResponseType.DONE_PARTIAL]
      }
      else if (messageType === BackendMessage.ReadyForQuery) {
         // const txStatus = readUint8(data, 5) as TransactionStatus
         return parseCompleted && bindingCompleted && commandCompleted
            ? [MessageHandlerResponseType.DONE_FINAL, { rows, rowsAffected }]
            : [MessageHandlerResponseType.FAIL, Error(`Failed to execute prepared query: ${!parseCompleted ? 'parsing' : !bindingCompleted ? 'binding' : 'command'} did not complete.`)]
      }
      else
         return [MessageHandlerResponseType.UNPROCESSED]
   })
}

// const enum TransactionStatus {
//   Idle                     = 73, // 'I'
//   InTransactionBlock       = 84, // 'T'
//   InFailedTransactionBlock = 69, // 'E'
// }

const enum MessageHandlerResponseType {
   UNPROCESSED,
   DONE_PARTIAL,
   DONE_FINAL,
   FAIL,
}

type MessageHandlerResponse<T>
   = [MessageHandlerResponseType.UNPROCESSED]
   | [MessageHandlerResponseType.DONE_PARTIAL]
   | [MessageHandlerResponseType.DONE_FINAL, T]
   | [MessageHandlerResponseType.FAIL, Error]

function handleBackendMessage<T>(conn: Socket, messageHandler: (messageType: BackendMessage, data: Buffer) => MessageHandlerResponse<T>) {
   return new Promise<BackendSuccess<T>>((resolve, reject) => {
      let dataLeftover: Buffer | undefined
      const notices: PostgresNotice[] = []
      const unexpected: UnexpectedBackendMessage[]  = []

      const onSuccess = (data: T) => resolve({ data, notices, unexpected })
      const onFailure = (error: Error) => reject({ error, notices, unexpected })
      const onComplete = () => {
         conn.off('data', onData)
         conn.off('error', onFailure)
         conn.off('close', onFailure)
      }

      conn.on('data', onData)
      conn.once('error', onFailure)
      conn.once('close', onFailure)

      function onData(data: Buffer) {
         if (dataLeftover) {
            data = Buffer.concat([dataLeftover, data])
            dataLeftover = undefined
         }

         if (data.byteLength <= 5) {
            dataLeftover = data
            return
         }

         const messageSize = 1 + readInt32(data, 1)
         if (messageSize > data.byteLength) {
            dataLeftover = data
            return
         }

         const messageType = readUint8(data, 0) as BackendMessage
         const res = messageHandler(messageType, data)
         if (res[0] === MessageHandlerResponseType.FAIL) {
            onComplete()
            onFailure(res[1])
         }
         else if (res[0] === MessageHandlerResponseType.DONE_FINAL) {
            onComplete()
            onSuccess(res[1])
         }
         else if (res[0] === MessageHandlerResponseType.UNPROCESSED) {
            if (messageType === BackendMessage.ErrorResponse) {
               onComplete()
               onFailure(new PostgresErrorOrNotice(data) as PostgresError)
            }
            else if (messageType === BackendMessage.NoticeResponse)
               notices.push(new PostgresErrorOrNotice(data) as PostgresNotice)
            else
               unexpected.push(new UnexpectedBackendMessage(messageType, data))
         }

         if (data.byteLength > messageSize)
            onData(data.slice(messageSize))
      }
   })
}

interface ColumnMetadata {
   name: string
   type: ObjectId
   tableId?: number
   positionInTable?: number
}

enum BackendMessage {
   Authentication           = 82,  // 'R'
   BackendKeyData           = 75,  // 'K'
   BindComplete             = 50,  // '2'
   CloseComplete            = 51,  // '3'
   CommandComplete          = 67,  // 'C'
   CopyBothResponse         = 87,  // 'W'
   CopyData                 = 100, // 'd'
   CopyDone                 = 99,  // 'c'
   CopyInResponse           = 71,  // 'G'
   CopyOutResponse          = 72,  // 'H'
   DataRow                  = 68,  // 'D'
   EmptyQueryResponse       = 73,  // 'I'
   ErrorResponse            = 69,  // 'E'
   FunctionCallResponse     = 86,  // 'V'
   NegotiateProtocolVersion = 118, // 'v'
   NoData                   = 110, // 'n'
   NoticeResponse           = 78,  // 'N'
   NotificationResponse     = 65,  // 'A'
   ParameterDescription     = 116, // 't'
   ParameterStatus          = 83,  // 'S'
   ParseComplete            = 49,  // '1'
   PortalSuspended          = 115, // 's'
   ReadyForQuery            = 90,  // 'Z'
   RowDescription           = 84,  // 'T'
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
