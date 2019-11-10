# pgeon

Extension for [node-postgres](https://github.com/brianc/node-postgres) and static analyzer for SQL queries.

## Installation

```sh
npm i -S pgeon
```

## Usage

### Runtime library

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

The `$query` template literal tag can be later used to identify SQL queries in the code for static analysis. For this reason, it is important that `$query` is not aliased, and that no other template literal named the same exists elsewhere in the codebase.

The usage of a template literal where the placeholders are assumed to be query parameters guarantees that the string is static, distinguishing them from dynamic queries, which are much harder to be statically analyzed.

### Static analyzer

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

Will output something like:

```sh
[syntax-error.ts:4] syntax error at or near "elect"
    elect one
    from dual
```

Similarly, you may have a select statement such as:

```ts
import {Client} from 'pg'
import 'pgeon'

interface Row {
    col: number
}

new Client().$query<Row>`
    select 'x' col
`
```

And be warned of the mistake:

```sh
[type-mismatch.ts:8] type mismatch in "Row.col": "number" and "TEXT" are incompatible
    select 'x' col
```

## Roadmap

Although the basic functionality is in place, covering a big chunk of use cases, there are a few more checks the tool could perform for increased reliability. *Contributions are welcomed!*

- Check types and nullability of `returning` clauses of `insert`/`update`/`delete` statements (see https://www.postgresql.org/docs/current/infoschema-columns.html and https://www.postgresql.org/docs/current/infoschema-attributes.html).
- Check types of placeholders which are part of `where` clauses of `select`/`update`/`delete` statements.
- Check types of placeholders which are part of `values`/`set` clauses of `insert`/`update` statements.
- Check nullability of selected columns in `select` statements.
- Support advanced TypeScript types such as union types, mapped types (e.g. `Partial<T>`), `true`, `1`, `1 | 2`... (compile a minimal TypeScript program to check for type subsets, e.g. `type x = 'abc' extends string ? true : false; let x: x = true`).
- Support statements beyond the basic CRUD or warn when used.
- Analyze supported types and JavaScript-to-Postgres type mappings in [pg-types](https://github.com/brianc/node-pg-types).
- Separate query parsing and validation logic for potential reuse by other clients (or even other languages).
- Complete `pg-query-native.d.ts` (see https://github.com/lfittl/libpg_query/issues/51).
