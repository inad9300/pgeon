# pgeon

Extension for node-postgres and static analyzer for SQL queries.

## Installation

```sh
npm i -S pgeon
```

## Usage

Importing `pgeon` after `pg` will extend `Pool` and `Client` with a new `$query` method, which is a proxy to the `query()` method already present in both `Pool` and `Client`.

```ts
import {Pool} from 'pg'
import 'pgeon'

export const pool = new Pool()
```

Technically, `$query` is a generic tagged template which returns a `Promise`. You can use it directly as such...

```ts
interface Row {
    date: Date
}

const result = await pool.$query<Row>`
    select now() as date
`
```

...or provide first a set of options which will be passed on to the original `query()` method.

```ts
interface User {
    id: number
    name: string
}

const userId = 123

const result = await pool.$query<User>({name: 'fetch-user'})`
    select id, name
    from user
    where id = ${userId}
`
```

All placeholders will be interpreted as query parameters, so that the resulting queries are not vulnerable to SQL injection. Having the parameters in-line helps minimizing indexing mistakes between the query placeholders and their corresponding values.

The `$query` template literal tag can be later used to identify SQL queries in the code for static analysis. For this reason, it is important that `$query` is not aliased, and that the same identifier is not used anywhere else in the codebase.

The usage of a template literal where the placeholders are assumed to be query parameters guarantees that the string is static, distinguishing them from dynamic queries, which are much harder to be statically analyzed.

With everything in place, the static analysis can now be run like so:

```sh
PGUSER=dbuser \
PGDATABASE=dbname \
PGPASSWORD=dbpass \
PGHOST=localhost \
PGPORT=5432 \
./node_modules/.bin/pgeon scan src/
```

The command-line tool will send to standard output any syntatic or semantic problems found in SQL statements, as well as any discrepancies between the types returned by the SQL query and those declared in TypeScript. If that is the case, a non-zero code will be returned.

For example, the following code:

```ts
import {Client} from 'pg'
import 'pgeon'

new Client().$query<{}>`
    elect one
    from dual
`
```

Will output something similar to:

```sh
[tests/syntax-error.ts:4] syntax error at or near "from"
    elect one
    from dual
```
