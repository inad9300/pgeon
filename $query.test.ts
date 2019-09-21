#!/usr/bin/env node

import {deepStrictEqual as eq} from 'assert'
import {Client, Pool, QueryResult, QueryResultRow} from 'pg'
import './$query'

(async () => {
    try {
        eq(typeof Client.prototype.$query, 'function')
        eq(typeof Pool.prototype.$query, 'function')

        eq(Client.prototype.$query, Pool.prototype.$query)
    } catch (err) {
        console.error(err.message)
        process.exit(-1)
    }

    const client = new Client
    const pool = new Pool

    try {
        eq(typeof client.$query, 'function')
        eq(typeof pool.$query, 'function')

        await client.connect()

        let result: QueryResult<QueryResultRow>

        result = await client.$query`select 1 a_number, 's' a_string`
        eq(result.rows, [{a_number: 1, a_string: 's'}])

        result = await client.$query({})`select 1 a_number, 's' a_string`
        eq(result.rows, [{a_number: 1, a_string: 's'}])

        result = await client.$query({})`select ${1}::integer a_number, ${'s'} a_string`
        eq(result.rows, [{a_number: 1, a_string: 's'}])

        result = await pool.$query`select 1 a_number, 's' a_string`
        eq(result.rows, [{a_number: 1, a_string: 's'}])

        result = await pool.$query({})`select 1 a_number, 's' a_string`
        eq(result.rows, [{a_number: 1, a_string: 's'}])

        result = await pool.$query({})`select ${1}::integer a_number, ${'s'} a_string`
        eq(result.rows, [{a_number: 1, a_string: 's'}])

        process.exit(0)
    } catch (err) {
        console.error(err.message)
    } finally {
        try {
            await client.end()
            await pool.end()
        } finally {
            process.exit(1)
        }
    }

    process.exit(1)
})()
