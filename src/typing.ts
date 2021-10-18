import { Pool, sql } from './pool'
import { ObjectId } from './ObjectId'
import type { QueryMetadata, Row } from './backend'

export async function getSourceWithQueryTypes(pool: Pool, source: string): Promise<string> {
   const matches = [...source.matchAll(/\bsql(<[^>]+?>)?`((?:\\.|[^`\\])*?)`/g)]
   if (matches.length === 0)
      return Promise.resolve(source)

   const promises = matches
      .reverse()
      .filter(match => {
         const line = source.substring(
            source.lastIndexOf('\n', match.index) + 1,
            source.indexOf('\n', match.index) - 1
         )
         return !line.trimLeft().startsWith('//')
      })
      .map(async match => {
         let i = 1
         const query = match[2].replace(/\$\{[^\}]+?\}/g, () => '$' + i++)
         const type = await getQueryType(pool, query)
         return { type, match }
      })

   for await (const { type, match } of promises) {
      const start = match.index! + 'sql'.length
      source = source.slice(0, start) + type + source.slice(start + (match[1]?.length || 0))
   }

   return source
}

export async function getQueryType(pool: Pool, query: string): Promise<string> {
   const meta = await pool.getQueryMetadata(query)
   const rowType = await getRowType(pool, meta)
   const paramsType = getParamsType(meta)
   return `<${rowType}, ${paramsType}>`
}

export async function getRowType(pool: Pool, { rowMetadata }: QueryMetadata): Promise<string> {
   const tableIds = rowMetadata.map(col => col.tableId).filter(x => !!x) as number[]
   const colPositions = rowMetadata.map(col => col.positionInTable).filter(x => !!x) as number[]

   const colNullability: Row[] = []
   if (tableIds.length > 0 && rowMetadata.length > 0)
      colNullability.push(
         ...(
            await pool.run(sql`
           select cls.oid, col.ordinal_position, col.is_nullable
           from information_schema.columns col
           join pg_catalog.pg_class cls on (cls.relname = col.table_name)
           where cls.oid = any(${tableIds}::int[])
           and col.ordinal_position = any(${colPositions})
         `)
         ).rows
      )

   const colTypes: string[] = []
   for (const col of rowMetadata) {
      const colName = /^[_a-zA-Z][_a-zA-Z0-9]*$/.test(col.name) ? col.name : `'${col.name}'`
      let colType = `${colName}: ${getTypeScriptType(col.type)}`
      if (col.tableId) {
         const cn = colNullability.find(r => r.oid === col.tableId && r.ordinal_position === col.positionInTable)
         if (cn?.is_nullable !== 'NO')
            colType += ' | null'
      } else {
         colType += ' | null'
      }
      colTypes.push(colType)
   }

   return '{ ' + colTypes.join(', ') + ' }'
}

export function getParamsType({ paramTypes }: QueryMetadata): string {
   return '[' + paramTypes.map(getTypeScriptType).join(', ') + ']'
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
