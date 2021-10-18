import { ok, deepStrictEqual as eq } from 'assert'
import { newPool, Client, QueryCancelledError, sql } from './pool'
import { ObjectId } from './ObjectId'

const start = process.hrtime()
const pool = newPool()

const tests: Promise<any>[] = []
let t = 0


// Public API.

tests[t++] = pool
  .run<{ one: 1, two: 2, three: 3 }, [2, 3]>(
    sql`select 1::int one, ${2}::int two, ${3}::int three`
  )
  .then(res => {
    eq(res.rowsAffected, 1)
    eq(res.rows.length, 1)
    eq(Object.keys(res.rows[0]), ['one', 'two', 'three'])
    eq(res.rows[0].one, 1)
    eq(res.rows[0].two, 2)
    eq(res.rows[0].three, 3)
  })

tests[t++] = pool
  .run<{ one: 1, two: 2, three: 3 }, [2, 3]>({
    sql: `select 1::int one, $1::int two, $2::int three`,
    params: [2, 3]
  })
  .then(res => {
    eq(res.rowsAffected, 1)
    eq(res.rows.length, 1)
    eq(Object.keys(res.rows[0]), ['one', 'two', 'three'])
    eq(res.rows[0].one, 1)
    eq(res.rows[0].two, 2)
    eq(res.rows[0].three, 3)
  })

tests[t++] = pool
  .run<{ one: 1, two: 2, three: 3 }, [2, 3]>({
    sql: `select 1::int one, $1::int two, $2::int three`,
    params: [2, 3],
    metadata: {
      paramTypes: [ObjectId.Int4, ObjectId.Int4],
      rowMetadata: [
        { type: ObjectId.Int4, name: 'one' },
        { type: ObjectId.Int4, name: 'two' },
        { type: ObjectId.Int4, name: 'three' },
      ]
    }
  })
  .then(res => {
    eq(res.rowsAffected, 1)
    eq(res.rows.length, 1)
    eq(Object.keys(res.rows[0]), ['one', 'two', 'three'])
    eq(res.rows[0].one, 1)
    eq(res.rows[0].two, 2)
    eq(res.rows[0].three, 3)
  })

tests[t++] = (async () => {
  const countEq = async (client: Client, expected: number) => {
    const { rows } = await client.run(sql`select count(*)::int count from things`)
    eq(rows[0].count, expected)
  }

  await pool.run(sql`drop table if exists things`)

  await pool.run(sql`create table things (name char(1) primary key)`)
  await countEq(pool, 0)

  const res = await pool.run(sql`insert into things (name) values ('a') returning *`)
  await countEq(pool, 1)
  eq(res.rows[0].name, 'a')

  await pool.transaction(async tx => {
    await tx.run(sql`insert into things (name) values (${'b'})`)
    await countEq(tx, 2)
  })
  await countEq(pool, 2)

  const rollbackTrigger = Symbol()
  try {
    await pool.transaction(async tx => {
      await tx.run(sql`insert into things (name) values ('c')`)
      await countEq(tx, 3)
      throw rollbackTrigger
    })
  } catch (err) {
    if (err !== rollbackTrigger) {
      throw err
    }
  }
  await countEq(pool, 2)
})()

tests[t++] = (async () => {
  const resultPromise = pool.run(sql`select * from pg_catalog.pg_type`)
  resultPromise.cancel()

  try {
    await resultPromise
    throw 'Failed to cancel query.'
  } catch (err) {
    ok(err instanceof QueryCancelledError)
  }
})()

tests[t++] = (async () => {
  try {
    await pool.transaction(async tx => {
      const resultPromise = tx.run(sql`select * from pg_catalog.pg_type`)
      resultPromise.cancel()
      await resultPromise
      throw 'Failed to cancel query.'
    })
    throw 'Failed to cancel query.'
  } catch (err) {
    ok(err instanceof QueryCancelledError) // FIXME Fails, sometimes.
  }
})()


// Serialization.

const MIN_INT_16 = -1 * 2 ** 16 / 2
const MAX_INT_16 =      2 ** 16 / 2 - 1

const MIN_UINT_32 = 0
const MAX_UINT_32 = 2 ** 32 - 1

const MIN_INT_32 = -1 * 2 ** 32 / 2
const MAX_INT_32 =      2 ** 32 / 2 - 1

const MIN_SAFE_INT = -1 * (2 ** 53 - 1) // Number.MIN_SAFE_INTEGER
const MAX_SAFE_INT =      (2 ** 53 - 1) // Number.MAX_SAFE_INTEGER

const MIN_FLOAT_32 = -1 * (2 - 2 ** -23) * 2 ** 127
const MAX_FLOAT_32 =      (2 - 2 ** -23) * 2 ** 127

const MIN_FLOAT_64 = -1 * (2 - 2 ** -52) * 2 ** 1023
const MAX_FLOAT_64 =      (2 - 2 ** -52) * 2 ** 1023 // Number.MAX_VALUE

const MIN_POSITIVE_FLOAT_64 = 5e-324 // Number.MIN_VALUE

// See https://postgresql.org/docs/current/datatype-numeric.html
const MAX_NUMERIC = '9'.repeat(131_072) + '.' + '9'.repeat(16_383)
const MIN_NUMERIC = '-' + MAX_NUMERIC

eq(parseFloat(MAX_NUMERIC), Infinity)
eq(parseFloat(MIN_NUMERIC), -Infinity)

tests[t++] = pool
  .run(sql`select ${false}::bool a, ${true}::bool b`)
  .then(({ rows }) => {
    eq(rows[0].a, false)
    eq(rows[0].b, true)
  })

tests[t++] = pool
  .run(sql`select ${MIN_INT_16}::int2 a, ${MAX_INT_16}::int2 b`)
  .then(({ rows }) => {
    eq(rows[0].a, MIN_INT_16)
    eq(rows[0].b, MAX_INT_16)
  })

tests[t++] = pool
  .run(sql`select ${MIN_INT_32}::int4 a, ${MAX_INT_32}::int4 b`)
  .then(({ rows }) => {
    eq(rows[0].a, MIN_INT_32)
    eq(rows[0].b, MAX_INT_32)
  })

tests[t++] = pool
  .run(sql`select ${MIN_UINT_32}::oid a, ${MAX_UINT_32}::oid b`)
  .then(({ rows }) => {
    eq(rows[0].a, MIN_UINT_32)
    eq(rows[0].b, MAX_UINT_32)
  })

tests[t++] = pool
  .run(sql`select ${BigInt(MIN_SAFE_INT) * 2n}::int8 a, ${BigInt(MAX_SAFE_INT) * 2n}::int8 b`)
  .then(({ rows }) => {
    eq(rows[0].a, BigInt(MIN_SAFE_INT) * 2n)
    eq(rows[0].b, BigInt(MAX_SAFE_INT) * 2n)
  })

tests[t++] = pool
  .run(sql`select ${MIN_FLOAT_32}::float4 a, ${MAX_FLOAT_32}::float4 b`)
  .then(({ rows }) => {
    eq(rows[0].a, MIN_FLOAT_32)
    eq(rows[0].b, MAX_FLOAT_32)
  })

tests[t++] = pool
  .run(sql`select ${MIN_FLOAT_64}::float8 a, ${MAX_FLOAT_64}::float8 b, ${MIN_POSITIVE_FLOAT_64}::float8 c`)
  .then(({ rows }) => {
    eq(rows[0].a, MIN_FLOAT_64)
    eq(rows[0].b, MAX_FLOAT_64)
    eq(rows[0].c, MIN_POSITIVE_FLOAT_64)
  })

tests[t++] = pool
  .run(sql`select ${NaN}::float4 a, ${Infinity}::float4 b, ${-Infinity}::float4 c, ${-0}::float4 d`)
  .then(({ rows }) => {
    eq(rows[0].a, NaN)
    eq(rows[0].b, Infinity)
    eq(rows[0].c, -Infinity)
    eq(rows[0].d, -0)
  })

tests[t++] = pool
  .run({
    sql: `
      select
        ${MAX_NUMERIC}::numeric a,
        ${MIN_NUMERIC}::numeric b,
        '1234567890.01234567890'::numeric c,
        '1.1'::numeric d,
        '10.01'::numeric e,
        '100.001'::numeric f,
        '1000.0001'::numeric g,
        '10000.00001'::numeric h,
        '100000.000001'::numeric i,
        '1000000.0000001'::numeric j,
        '10000000.00000001'::numeric k,
        '100000000.000000001'::numeric l,
        '1000000000.0000000001'::numeric m,
        '10000000000.00000000001'::numeric o,
        '100000000000.000000000001'::numeric p,
        '1000000000000.0000000000001'::numeric q,
        '100000000000000000000000000000000000000000000000000000'::numeric r,
        '0.00000002'::numeric s,
        '0.123400000000'::numeric t
    `
  })
  .then(({ rows }) => {
    eq(rows[0].a, MAX_NUMERIC)
    eq(rows[0].b, MIN_NUMERIC)
    eq(rows[0].c, '1234567890.01234567890')
    eq(rows[0].d, '1.1')
    eq(rows[0].e, '10.01')
    eq(rows[0].f, '100.001')
    eq(rows[0].g, '1000.0001')
    eq(rows[0].h, '10000.00001')
    eq(rows[0].i, '100000.000001')
    eq(rows[0].j, '1000000.0000001')
    eq(rows[0].k, '10000000.00000001')
    eq(rows[0].l, '100000000.000000001')
    eq(rows[0].m, '1000000000.0000000001')
    eq(rows[0].o, '10000000000.00000000001')
    eq(rows[0].p, '100000000000.000000000001')
    eq(rows[0].q, '1000000000000.0000000000001')
    eq(rows[0].r, '100000000000000000000000000000000000000000000000000000')
    eq(rows[0].s, '0.00000002')
    eq(rows[0].t, '0.123400000000')
  })

tests[t++] = pool
  .run(sql`
    select
      ${MAX_NUMERIC}::numeric a,
      ${MIN_NUMERIC}::numeric b,
      ${'-1234567890.01234567890'}::numeric c,
      ${'-1.1'}::numeric d,
      ${'-10.01'}::numeric e,
      ${'-100.001'}::numeric f,
      ${'-1000.0001'}::numeric g,
      ${'-10000.00001'}::numeric h,
      ${'-100000.000001'}::numeric i,
      ${'-1000000.0000001'}::numeric j,
      ${'-10000000.00000001'}::numeric k,
      ${'-100000000.000000001'}::numeric l,
      ${'-1000000000.0000000001'}::numeric m,
      ${'-10000000000.00000000001'}::numeric o,
      ${'-100000000000.000000000001'}::numeric p,
      ${'-1000000000000.0000000000001'}::numeric q,
      ${'-100000000000000000000000000000000000000000000000000000'}::numeric r,
      ${'-0.00000002'}::numeric s,
      ${'-0.123400000000'}::numeric t
  `)
  .then(({ rows }) => {
    eq(rows[0].a, MAX_NUMERIC)
    eq(rows[0].b, MIN_NUMERIC)
    eq(rows[0].c, '-1234567890.01234567890')
    eq(rows[0].d, '-1.1')
    eq(rows[0].e, '-10.01')
    eq(rows[0].f, '-100.001')
    eq(rows[0].g, '-1000.0001')
    eq(rows[0].h, '-10000.00001')
    eq(rows[0].i, '-100000.000001')
    eq(rows[0].j, '-1000000.0000001')
    eq(rows[0].k, '-10000000.00000001')
    eq(rows[0].l, '-100000000.000000001')
    eq(rows[0].m, '-1000000000.0000000001')
    eq(rows[0].o, '-10000000000.00000000001')
    eq(rows[0].p, '-100000000000.000000000001')
    eq(rows[0].q, '-1000000000000.0000000000001')
    eq(rows[0].r, '-100000000000000000000000000000000000000000000000000000')
    eq(rows[0].s, '-0.00000002')
    eq(rows[0].t, '-0.123400000000')
  })

tests[t++] = pool
  .run({ sql: `select 'NaN'::numeric a, '0'::numeric b, '-0'::numeric c, '.5'::numeric d, '-.5'::numeric e, '0.0'::numeric f` })
  .then(({ rows }) => {
    eq(rows[0].a, 'NaN')
    eq(rows[0].b, '0')
    eq(rows[0].c, '0')
    eq(rows[0].d, '0.5')
    eq(rows[0].e, '-0.5')
    eq(rows[0].f, '0.0')
    // eq(rows[0].g, 'Infinity')
    // eq(rows[0].h, '-Infinity')
  })

tests[t++] = pool
  .run(sql`select ${'NaN'}::numeric a, ${'0'}::numeric b, ${'-0'}::numeric c, ${'.5'}::numeric d, ${'-.5'}::numeric e, ${'0.0'}::numeric f`)
  .then(({ rows }) => {
    eq(rows[0].a, 'NaN')
    eq(rows[0].b, '0')
    eq(rows[0].c, '0')
    eq(rows[0].d, '0.5')
    eq(rows[0].e, '-0.5')
    eq(rows[0].f, '0.0')
    // eq(rows[0].g, 'Infinity')
    // eq(rows[0].h, '-Infinity')
  })

tests[t++] = pool
  .run(sql`select timestamp '2004-10-19T10:23:54.021Z' a, timestamptz '2004-10-19T10:23:54.021Z' b`)
  .then(({ rows }) => {
    eq(rows[0].a, new Date('2004-10-19T10:23:54.021Z'))
    eq(rows[0].b, new Date('2004-10-19T10:23:54.021Z'))
  })

tests[t++] = pool
  .run(sql`select ${new Date('2004-10-19T10:23:54.021Z')}::timestamp a, ${new Date('2004-10-19T10:23:54.021Z')}::timestamptz b`)
  .then(({ rows }) => {
    eq(rows[0].a, new Date('2004-10-19T10:23:54.021Z'))
    eq(rows[0].b, new Date('2004-10-19T10:23:54.021Z'))
  })

tests[t++] = pool
  .run(sql`select ${' '}::char a, ${'x'}::char b, ${'🤓'}::char c`)
  .then(({ rows }) => {
    eq(rows[0].a, ' ')
    eq(rows[0].b, 'x')
    eq(rows[0].c, '🤓')
  })

tests[t++] = pool
  .run(sql`select ${''}::varchar a, ${'unknown'}::varchar b, ${'àáâäæãåā🤓'}::varchar c`)
  .then(({ rows }) => {
    eq(rows[0].a, '')
    eq(rows[0].b, 'unknown')
    eq(rows[0].c, 'àáâäæãåā🤓')
  })

tests[t++] = pool
  .run(sql`select ${''}::text a, ${'unknown'}::text b, ${'àáâäæãåā🤓'}::text c`)
  .then(({ rows }) => {
    eq(rows[0].a, '')
    eq(rows[0].b, 'unknown')
    eq(rows[0].c, 'àáâäæãåā🤓')
  })

tests[t++] = pool
  .run(sql`select ${''}::bpchar a, ${'unknown'}::bpchar b, ${'àáâäæãåā🤓'}::bpchar c`)
  .then(({ rows }) => {
    eq(rows[0].a, '')
    eq(rows[0].b, 'unknown')
    eq(rows[0].c, 'àáâäæãåā🤓')
  })

tests[t++] = pool
  .run(sql`select ${''}::name a, ${'unknown'}::name b, ${'àáâäæãåā🤓'}::name c`)
  .then(({ rows }) => {
    eq(rows[0].a, '')
    eq(rows[0].b, 'unknown')
    eq(rows[0].c, 'àáâäæãåā🤓')
  })

tests[t++] = pool
  .run(sql`
    select
      array['x', 'y', 'z']::char[] a,
      array['x', 'y', 'z']::varchar[] b,
      array['x', 'y', 'z']::text[] c,
      array['x', 'y', 'z']::bpchar[] d,
      array['x', 'y', 'z']::name[] e
  `)
  .then(({ rows }) => {
    eq(rows[0].a, ['x', 'y', 'z'])
    eq(rows[0].b, ['x', 'y', 'z'])
    eq(rows[0].c, ['x', 'y', 'z'])
    eq(rows[0].d, ['x', 'y', 'z'])
    eq(rows[0].e, ['x', 'y', 'z'])
  })

tests[t++] = pool
  .run(sql`select ${[]}::int2[] a, ${[-2, -1, 0, 1, 42]}::int2[] b`)
  .then(({ rows }) => {
    eq(rows[0].a, [])
    eq(rows[0].b, [-2, -1, 0, 1, 42])
  })

tests[t++] = pool
  .run(sql`select ${[]}::int4[] a,  ${[-1, 0, 1, 42]}::int4[] b`)
  .then(({ rows }) => {
    eq(rows[0].a, [])
    eq(rows[0].b, [-1, 0, 1, 42])
  })

tests[t++] = pool
  .run(sql`select ${[]}::int8[] a,  ${[-1n, 0n, 1n, 42n]}::int8[] b`)
  .then(({ rows }) => {
    eq(rows[0].a, [])
    eq(rows[0].b, [-1n, 0n, 1n, 42n])
  })

tests[t++] = pool
  .run(sql`select ${[]}::float4[] a,  ${[-1.5, 0.0, 1.0, 42.25]}::float4[] b`)
  .then(({ rows }) => {
    eq(rows[0].a, [])
    eq(rows[0].b, [-1.5, 0.0, 1.0, 42.25])
  })

tests[t++] = pool
  .run(sql`select ${[]}::float8[] a,  ${[-1.5, 0.0, 1.0, 42.25]}::float8[] b`)
  .then(({ rows }) => {
    eq(rows[0].a, [])
    eq(rows[0].b, [-1.5, 0.0, 1.0, 42.25])
  })

tests[t++] = pool
  .run(sql`
    select
      ${Buffer.of()}::bytea a,
      ${Buffer.of(0, 1, 42)}::bytea b,
      ${Buffer.from('🍅🥔', 'utf8')}::bytea c
  `)
  .then(({ rows }) => {
    eq(rows[0].a, Buffer.of())
    eq(rows[0].b, Buffer.of(0, 1, 42))
    eq(rows[0].c.toString('utf8'), '🍅🥔')
  })

tests[t++] = pool
  .run(sql`
    select
      '5'::json a,
      '"5"'::json b,
      'null'::json c,
      'true'::json d,
      '[{ "x": 0 }]'::json e
  `)
  .then(({ rows }) => {
    eq(rows[0].a, 5)
    eq(rows[0].b, '5')
    eq(rows[0].c, null)
    eq(rows[0].d, true)
    eq(rows[0].e, [{ x: 0 }])
  })

tests[t++] = pool
  .run(sql`
    select
      '5'::jsonb a,
      '"5"'::jsonb b,
      'null'::jsonb c,
      'true'::jsonb d,
      '[{ "x": 0 }]'::jsonb e
  `)
  .then(({ rows }) => {
    eq(rows[0].a, 5)
    eq(rows[0].b, '5')
    eq(rows[0].c, null)
    eq(rows[0].d, true)
    eq(rows[0].e, [{ x: 0 }])
  })

tests[t++] = pool
  .run(sql`
    select
      ${5}::json a,
      ${'5'}::json b,
      ${null}::json c,
      ${true}::json d,
      ${[{ "x": 0 }]}::json e
  `)
  .then(({ rows }) => {
    eq(rows[0].a, 5)
    eq(rows[0].b, '5')
    eq(rows[0].c, null)
    eq(rows[0].d, true)
    eq(rows[0].e, [{ x: 0 }])
  })

tests[t++] = pool
  .run(sql`
    select
      ${5}::jsonb a,
      ${'5'}::jsonb b,
      ${null}::jsonb c,
      ${true}::jsonb d,
      ${[{ "x": 0 }]}::jsonb e
  `)
  .then(({ rows }) => {
    eq(rows[0].a, 5)
    eq(rows[0].b, '5')
    eq(rows[0].c, null)
    eq(rows[0].d, true)
    eq(rows[0].e, [{ x: 0 }])
  })


// Kickoff.

Promise
  .all(tests)
  .then(() => exit(0))
  .catch(err => exit(1, err))
  .finally(pool.destroy)

function exit(code: 0): void
function exit(code: 1, err: any): void
function exit(code: 0 | 1, err?: any) {
  const end = process.hrtime(start)
  code
    ? console.error('👎', err)
    : console.log('👍', `${end[0] * 1_000 + end[1] / 1_000_000} ms`)
  process.exit(code)
}
