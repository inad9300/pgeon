import {Client} from 'pg'

;(async () => {
    interface Row {
        a_number: number
        a_string: string
    }

    await new Client().$query<Row>`
        select 1 a_number, 's' a_string
    `
})
