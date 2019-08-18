import * as pg from 'pg'

// TODO Verify types (e.g. `Column`) aren't too restrictive.

const pool = new pg.Pool

;(async () => {
    interface User {
        id: string
        name: string
    }
    const id = 5
    const users = await pool.$query<User>({})`
        select *
        from users
        where id = ${id}
    `
    console.log(users)
})
