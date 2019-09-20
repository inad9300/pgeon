import {Client} from 'pg'
import '../$query'

interface Row {
    a_number: number
    a_string: string
}

new Client().$query<Row>`
    elect one
    from dual
`
