import {Client} from 'pg'

;(async () => {
    interface Row {
        a_number: number
        a_string: string
    }

    await new Client().$query<Row>`
        elect one
        from dual
    `
})
