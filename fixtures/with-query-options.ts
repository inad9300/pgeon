import {Client} from 'pg'
import '../$query'

interface Row {
    a_number: number
    a_string: string
}

new Client().$query<Row>({name: 'example'})`
    select 1 a_number, 's' a_string
`
