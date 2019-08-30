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

    // const {$query} = pool
    const id = 5
    const users = await pool.$query<User>/*({})*/`
        select *
        from users
        where id = ${id}
        and 1 = 2
        and id = ${id}
        and 3 = 4
    `

    console.log(users)
})
