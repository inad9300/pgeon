import {Client} from 'pg'

;(async () => {
    interface Row {
        a_number: number
        a_string: string
    }

    await new Client().$query<Row>({name: 'example'})`
        select 1 a_number, 's' a_string
    `
})
