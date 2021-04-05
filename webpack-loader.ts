import { newPool, getTypeScriptType, QueryMetadata, Row } from './postgres-client'
import type { Compiler } from 'webpack'

const pool = newPool()

let closeScheduled = false

export default function pgeonLoader(this: any, source: string) {
  const callback = this.async()

  if (!closeScheduled) {
    closeScheduled = true
    const compiler = this._compiler as Compiler
    compiler.hooks.done.tap('hooks::done', () => pool.close())
  }

  getSourceWithQueryTypes(source).then(typedSource => callback(null, typedSource))
}

async function getSourceWithQueryTypes(source: string): Promise<string> {
  const matches = [...source.matchAll(/\brunStaticQuery(<[^>]+?>)?`((?:\\.|[^`\\])*?)`/g)]
  if (matches.length === 0) {
    return Promise.resolve(source)
  }

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
      const type = await getQueryType(query)
      return { type, match }
    })

  for await (const { type, match } of promises) {
    const start = match.index! + 'runStaticQuery'.length
    source = source.slice(0, start) + type + source.slice(start + (match[1]?.length || 0))
  }

  return source
}

async function getQueryType(query: string): Promise<string> {
  const meta = await pool.getQueryMetadata(query)
  const rowType = await getRowType(meta)
  const paramsType = getParamsType(meta)
  return `<${rowType}, ${paramsType}>`
}

async function getRowType({ columnMetadata }: QueryMetadata): Promise<string> {
  const tableIds = columnMetadata.map(col => col.tableId).filter(x => !!x) as number[]
  const colPositions = columnMetadata.map(col => col.positionInTable).filter(x => !!x) as number[]

  const colNullability: Row[] = []
  if (tableIds.length > 0 && columnMetadata.length > 0) {
    colNullability.push(
      ...(
        await pool.runStaticQuery`
          select cls.oid, col.ordinal_position, col.is_nullable
          from information_schema.columns col
          join pg_catalog.pg_class cls on (cls.relname = col.table_name)
          where cls.oid = any(${tableIds}::int[])
          and col.ordinal_position = any(${colPositions})
        `
      ).rows
    )
  }

  const colTypes: string[] = []
  for (const col of columnMetadata) {
    const colName = /^[_a-zA-Z][_a-zA-Z0-9]*$/.test(col.name) ? col.name : `'${col.name}'`
    let colType = `${colName}: ${getTypeScriptType(col.type)}`
    if (col.tableId) {
      const cn = colNullability.find(r => r.oid === col.tableId && r.ordinal_position === col.positionInTable)
      if (cn?.is_nullable !== 'NO') {
        colType += ' | null'
      }
    } else {
      colType += ' | null'
    }
    colTypes.push(colType)
  }

  return '{ ' + colTypes.join(', ') + ' }'
}

function getParamsType({ paramTypes }: QueryMetadata): string {
  return '[' + paramTypes.map(getTypeScriptType).join(', ') + ']'
}
