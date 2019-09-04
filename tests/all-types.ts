import {Client} from 'pg'

;(async () => {
    interface Row {
        a_number: number
        a_string: string
        a_nullable_string: string | null
        a_date: Date
    }

    await new Client().$query<Row>`
        select 1 a_number, 's' a_string, quote_literal(null) a_nullable_string, now() a_date
    `
})
