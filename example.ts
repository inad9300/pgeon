import { newPool } from './postgres-client'

const pool = newPool()

const limit = 2

pool.runStaticQuery`
  select oid
  from pg_catalog.pg_class
  limit ${limit}::int4
`
.then(res => {
  setTimeout(pool.close)
  console.debug('Query result:', res)

  const oid: string = res.rows[0].oid
  return oid
})
