import { createHash, createHmac, pbkdf2Sync } from 'crypto'
import type { ColumnValue } from './backend'
import { ObjectId } from './ObjectId'
import {
   writeArray,
   writeBuffer,
   writeCString,
   writeFloat32,
   writeFloat64,
   writeInt16,
   writeInt32,
   writeInt64,
   writeNumeric,
   writeTimestamp,
   writeUint32,
   writeUint8,
   writeUtf8String
} from './serialization'

// References:
// - https://postgresql.org/docs/current/protocol.html
// - https://postgresql.org/docs/current/datatype.html
// - https://beta.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf
// - https://segmentfault.com/a/1190000017136059
// - https://github.com/postgres/postgres/tree/master/src/backend/libpq
// - https://github.com/postgres/postgres/tree/master/src/backend/utils/adt

export const sslRequestMessage = createSslRequestMessage()

function createSslRequestMessage(): Buffer {
   // 8 = 4 (message size) + 4 (SSL request code)
   const size = 8
   const message = Buffer.allocUnsafe(size)
   writeInt32(message, size, 0)
   writeInt32(message, 8087_7103, 4)
   return message
}

export function createStartupMessage(username: string, database: string): Buffer {
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

export function createCancelRequestMessage(processId: number, secretKey: number): Buffer {
   const message = Buffer.allocUnsafe(16)
   writeInt32(message, 16, 0)
   writeInt32(message, 8087_7102, 4)
   writeInt32(message, processId, 8)
   writeInt32(message, secretKey, 12)
   return message
}

export function createCleartextPasswordMessage(password: string) {
   // 6 = 1 (message type) + 4 (message size) + 1 (password null terminator)
   const size = 6 + Buffer.byteLength(password)
   const message = Buffer.allocUnsafe(size)
   writeUint8(message, FrontendMessage.PasswordMessage, 0)
   writeInt32(message, size - 1, 1)
   writeCString(message, password, 5)
   return message
}

export function createMd5PasswordMessage(username: string, password: string, salt: Buffer) {
   const credentialsMd5 = 'md5' + md5(Buffer.concat([Buffer.from(md5(password + username)), salt]))

   // 6 = 1 (message type) + 4 (message size) + 1 (credentialsMd5 null terminator)
   const size = 6 + Buffer.byteLength(credentialsMd5)
   const message = Buffer.allocUnsafe(size)

   writeUint8(message, FrontendMessage.PasswordMessage, 0)
   writeInt32(message, size - 1, 1)
   writeCString(message, credentialsMd5, 5)

   return message
}

export function createSaslInitialResponseMessage(username: string, nonce: string) {
   // 55 = 1 (message type) + 4 (message size) + 1 (password null terminator) + 4 (size of initial response) + 13 ('SCRAM-SHA-256'.length) + ??
   const size = 55 + username.length
   const message = Buffer.allocUnsafe(size)

   writeUint8(message, FrontendMessage.PasswordMessage, 0)
   writeInt32(message, size - 1, 1)
   writeCString(message, 'SCRAM-SHA-256', 5)
   writeInt32(message, 32 + username.length, 19)
   writeCString(message, `n,,n=${username},r=${nonce}`, 23)

   return message
}

export function createSaslContinueResponseMessage(username: string, password: string, nonce: string, serverFirstMessage: string) {
   const parts = serverFirstMessage.split(',')
   const r = parts[0].slice(2)
   const s = parts[1].slice(2)
   const i = parts[2].slice(2)

   // Reference: https://datatracker.ietf.org/doc/html/rfc5802#section-3
   const saltedPassword = pbkdf2Sync(password, Buffer.from(s, 'base64'), parseInt(i, 10), 32, 'sha256')
   const clientKey = hmac(saltedPassword, 'Client Key')
   const storedKey = sha256(clientKey)
   const clientFirstMessageBare = `n=${username},r=${nonce}`
   const clientFinalMessageWithoutProof = `c=biws,r=${r}`
   const authMessage = `${clientFirstMessageBare},${serverFirstMessage},${clientFinalMessageWithoutProof}`
   const clientSignature = hmac(storedKey, authMessage)
   const clientProof = xor(clientKey, clientSignature).toString('base64')
   const serverKey = hmac(saltedPassword, 'Server Key')
   const serverSignature = hmac(serverKey, authMessage).toString('base64')

   // 109 = 1 (message type) + 4 (message size) + 104 (`response.length`)
   const message = Buffer.allocUnsafe(109)

   writeUint8(message, FrontendMessage.PasswordMessage, 0)
   writeInt32(message, 108, 1)
   writeUtf8String(message, `${clientFinalMessageWithoutProof},p=${clientProof}`, 5)

   return [message, serverSignature] as const
}

export function createSimpleQueryMessage(querySql: string): Buffer {
   // 6 = 1 (message type) + 4 (message size) + 1 (query null terminator)
   const size = 6 + Buffer.byteLength(querySql)
   const message = Buffer.allocUnsafe(size)
   writeUint8(message, FrontendMessage.Query, 0)
   writeInt32(message, size - 1, 1)
   writeCString(message, querySql, 5)
   return message
}

export function createParseMessage(querySql: string, queryId: string, paramTypes: ObjectId[]): Buffer {
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

export function createDescribeMessage(type: DescribeType, id: string): Buffer {
   // 7 = 1 (message type) + 4 (message size) + 1 (describe message type) + 1 (queryId null terminator)
   const size = 7 + Buffer.byteLength(id)
   const message = Buffer.allocUnsafe(size)
   writeUint8(message, FrontendMessage.Describe, 0)
   writeInt32(message, size - 1, 1)
   writeUint8(message, type, 5)
   writeCString(message, id, 6)
   return message
}

export const syncMessage = createSyncMessage()

function createSyncMessage(): Buffer {
   // 5 = 1 (message type) + 4 (message size)
   const size = 5
   const message = Buffer.allocUnsafe(size)
   writeUint8(message, FrontendMessage.Sync, 0)
   writeInt32(message, size - 1, 1)
   return message
}

export function createBindMessage(queryId: string, params: ColumnValue[], paramTypes: ObjectId[], portal: string): Buffer {
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

export const executeUnnamedPortalMessage = createExecuteMessage('')

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

export function md5(data: string | Buffer) {
   return createHash('md5').update(data).digest('hex')
}

function sha256(data: string | Buffer) {
   return createHash('sha256').update(data).digest()
}

function hmac(key: string | Buffer, data: string | Buffer) {
   return createHmac('sha256', key).update(data).digest()
}

function xor(a: Buffer, b: Buffer) {
   const result = Buffer.allocUnsafe(a.length)
   for (let i = 0; i < a.length; ++i) {
      result[i] = a[i] ^ b[i]
   }
   return result
}

const enum FrontendMessage {
   Bind            = 66,  // 'B'
   Close           = 67,  // 'C'
   CopyData        = 100, // 'd'
   CopyDone        = 99,  // 'c'
   Describe        = 68,  // 'D'
   Execute         = 69,  // 'E'
   Flush           = 72,  // 'H'
   FunctionCall    = 70,  // 'F'
   Parse           = 80,  // 'P'
   PasswordMessage = 112, // 'p'
   Query           = 81,  // 'Q'
   Sync            = 83,  // 'S'
   Terminate       = 88,  // 'X'
}

export enum BackendMessage {
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

export const enum DescribeType {
   Portal            = 80, // 'P'
   PreparedStatement = 83, // 'S'
}

const enum WireFormat {
   Text   = 0,
   Binary = 1,
}
