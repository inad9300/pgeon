import {Client} from 'pg'

;(async () => {
    interface Row {
        a_number: string
    }

    await new Client().$query<Row>`
        select 1 a_number
    `
})
