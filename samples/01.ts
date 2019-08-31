import * as pg from 'pg'

const pool = new pg.Pool

;(async () => {
    interface User {
        id: number
        name: string
        birthday: Date
    }

    const id = 5
    const users = await pool.$query<User>`
        select 1 number, now() date, 'something' optional, quote_literal(null) nullable_string
        where 1 = ${id}
    `

    console.log(users)
})
