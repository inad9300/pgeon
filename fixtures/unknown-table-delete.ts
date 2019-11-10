import {Client} from 'pg'
import '../$query'

interface Row {
    a: number
    b: number
    c: number
}

new Client().$query<Row>`
    delete from t
    where stale = TRUE
    returning *
`
