import {Client} from 'pg'
import '../$query'

interface Row {
    sql_language_programming_language: string
    sql_language_year: string
}

new Client().$query<Row>`
    update information_schema.sql_languages
    set sql_language_programming_language = 'Rust'
    where sql_language_year = '2020'
    returning sql_language_programming_language, sql_language_year
`
