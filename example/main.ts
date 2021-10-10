import { newPool, sql } from 'pgeon/postgres-client'

const pool = newPool()
const limit = 2

pool.run(sql`
   select oid
   from pg_catalog.pg_class
   limit ${limit}::int4
`)
.then(res => {
   console.debug('Query result:', res)
   const oid: string = res.rows[0].oid
   return oid
})
.finally(pool.destroy)
