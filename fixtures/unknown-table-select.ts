import {Client} from 'pg'
import '../$query'

interface Row {
    a_number: number
    a_string: string
}

new Client().$query<Row>`
    select *
    from users
    where id = 5
`
