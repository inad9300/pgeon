import * as pg from 'pg'

const pool = new pg.Pool

;(async () => {
    type int = number

    interface B {
        b: boolean
    }

    interface User {
        id: int
        name: string
        S: String
        things?: any[]
        others: Array<symbol>
        b: B
        d: Date
        u: Uint8Array | null
    }

    const id = 5
    const users = await pool.$query<User>({})`
        select *
        from users
        where id = ${id}
        and 1 = 2
        and id = ${id}
        and 3 = 4
    `

    console.log(users)
})
