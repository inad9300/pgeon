import {Client} from 'pg'
import '../$query'

interface Row {
    a: number
    b: number
    c: number
}

new Client().$query<Row>`
    update t
    set a = 1, b = 2, c = 3
    where x = 'y'
    returning *
`
