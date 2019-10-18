import {Client} from 'pg'
import '../$query'

interface Row {
    a_boolean: boolean
    a_number: number
    a_string: string
    a_nullable_string: string | null
    a_date: Date
}

new Client().$query<Row>`
    select
        true a_boolean,
        1 a_number,
        's' a_string,
        quote_literal(null) a_nullable_string,
        now() a_date
`
