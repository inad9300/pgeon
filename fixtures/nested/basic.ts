import {Client} from 'pg'
import '../../$query'

new Client().$query<{}>`
    elect 1
`
