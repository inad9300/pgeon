import {Client} from 'pg'
import '../$query'

interface Row {
    a_number: string
}

new Client().$query<Row>`
    select 1 a_number
`
