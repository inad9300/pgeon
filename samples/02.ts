import * as pg from 'pg'

const pool = new pg.Pool

;(async () => {
    type int = number

    interface B {
        b: boolean
    }

    interface User {
        id: undefined | int | string
        name: string
        S: String
        bi: BigInt
        things?: any[]
        others: Array<symbol>
        b: B
        d: Date
        u: number | null | Uint8Array | null
    }

    const {$query} = pool
    const id = 5
    const users = await pool.$query<User>({})`
        select *
        from users
        where id = ${id + `${0 * 2}` + id}
        and 1 = 1
        and id in (${id - 5})
        and 2 = 2
    `

    console.log(users)
})
