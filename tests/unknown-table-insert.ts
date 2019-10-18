import {Client} from 'pg'
import '../$query'

interface Row {
    a: number
    b: number
    c: number
}

new Client().$query<Row>`
    with x as (
        select 0
    )
    insert into t (a, b, c)
    values (1, 2, 3)
    returning *
`
