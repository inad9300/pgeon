import {Client} from 'pg'
import '../$query'

interface RowA {
    a_boolean: boolean
    a_number: number
}

interface RowB {
    a_string: string
}

new Client().$query<RowA & RowB>`
    select
        true a_boolean,
        1 a_number,
        's' a_string
`
