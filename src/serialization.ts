import { ObjectId } from './ObjectId'

export function readUint8(buffer: Buffer, offset: number): number {
   return buffer[offset]
}

export function writeUint8(buffer: Buffer, value: number, offset: number): number {
   buffer[offset++] = value
   return offset
}

export function readInt16(buffer: Buffer, offset: number): number {
   const value = (buffer[offset] << 8) + buffer[offset + 1]
   return value | (value & 32768) * 0x1fffe
}

export function writeInt16(buffer: Buffer, value: number, offset: number): number {
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

export function readInt32(buffer: Buffer, offset: number): number {
   return (buffer[offset] << 24)
      + (buffer[++offset] << 16)
      + (buffer[++offset] << 8)
      + buffer[++offset]
}

export function writeInt32(buffer: Buffer, value: number, offset: number): number {
   buffer[offset++] = value >> 24
   buffer[offset++] = value >> 16
   buffer[offset++] = value >> 8
   buffer[offset++] = value
   return offset
}

export function readUint32(buffer: Buffer, offset: number): number {
   return buffer[offset] * 16_777_216
      + (buffer[++offset] << 16)
      + (buffer[++offset] << 8)
      + buffer[++offset]
}

export function writeUint32(buffer: Buffer, value: number, offset: number): number {
   buffer[offset++] = value >> 24
   buffer[offset++] = value >> 16
   buffer[offset++] = value >> 8
   buffer[offset++] = value
   return offset
}

export function readInt64(buffer: Buffer, offset: number): bigint {
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

export function writeInt64(buffer: Buffer, value: bigint, offset: number): number {
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

export const readFloat32 = bigEndian ? function readFloat32(buffer: Buffer, offset: number): number {
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

export const writeFloat32 = bigEndian ? function writeFloat32(buffer: Buffer, value: number, offset: number): number {
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

export const readFloat64 = bigEndian ? function readFloat64(buffer: Buffer, offset: number): number {
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

export const writeFloat64 = bigEndian ? function writeFloat64(buffer: Buffer, value: number, offset: number): number {
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
export function readNumeric(buffer: Buffer, offset: number): string {
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

   if (digitsInBuffer === 0 || wholesCount <= 0)
      result += '0'
   else {
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
export function writeNumeric(buffer: Buffer, value: string, offset: number): number {
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

export function readTimestamp(buffer: Buffer, offset: number): Date {
   const value = 4_294_967_296 * readInt32(buffer, offset) + readUint32(buffer, offset + 4)
   return new Date(Math.round(value / 1_000) + postgresEpoch)
}

export function writeTimestamp(buffer: Buffer, value: Date, offset: number): number {
   const t = (value.getTime() - postgresEpoch) * 1_000
   offset = writeInt32(buffer, t / 4_294_967_296, offset)
   return writeUint32(buffer, t, offset)
}

export function readUtf8String(buffer: Buffer, offset: number, size: number): string {
   return buffer.slice(offset, offset + size).toString('utf8')
}

export function writeUtf8String(buffer: Buffer, value: string, offset: number): number {
   return offset + buffer.write(value, offset)
}

export function readCString(buffer: Buffer, offset: number): string {
   let end = offset
   while (buffer[end] !== 0) ++end
   return buffer.slice(offset, end).toString('ascii')
}

export function writeCString(buffer: Buffer, value: string, offset: number): number {
   offset += buffer.write(value, offset, 'ascii')
   buffer[offset++] = 0
   return offset
}

export function writeBuffer(buffer: Buffer, value: Buffer, offset: number): number {
   value.copy(buffer, offset)
   return offset + value.length
}

export function readArray<T>(buffer: Buffer, readElem: (buffer: Buffer, offset: number, size: number) => T): T[] {
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

export function writeArray<T>(buffer: Buffer, values: T[], offset: number, elemType: ObjectId, writeElem: (buffer: Buffer, value: T, offset: number) => number): number {
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
