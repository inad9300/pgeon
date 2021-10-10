_Work in progress..._

# pgeon

This library is:
1. A zero-dependencies, decently fast Postgres client.
2. A webpack loader that type-checks your SQL queries at compile time (seriously).


## Installation

```sh
npm install --save pgeon
```


## Example

For a first look, check out the small [example application](./example.ts) and its corresponding [webpack configuration](./example.webpack.config.ts). Provided Docker is running, you can try starting it with the command below. Notice that there is a type error which is caught _at compile time!_

```sh
./docker-npm run example
```


## Postgres client

A from-scratch implementation of the Postgres protocol covering most common use cases. Its features include:
- Connection pooling
- Secure database connections
- Support for most common data types (with more in the horizon)
- Well-defined mapping between Postgres and JavaScript types
- Query cancellation
- Basic transaction management
- Usage of Postgres' binary data format
- Query preparation for SQL injection prevention

### Connection pool

The first step towards executing useful queries is starting a connection pool.

```ts
import { newPool } from 'pgeon/postgres-client'

export const db = newPool()
```

If no options are provided, [standard Postgres environmental variables](https://postgresql.org/docs/current/libpq-envars.html) will be read and default values used. Explicit parameters can be provided to configure the database connection and pool limits.

```ts
import { newPool } from 'pgeon/postgres-client'

export const db = newPool({
   host: 'https://example.org',
   port: 41100,
   username: 'john_doe',
   password: 'aV27FGH!!bVxpQyyBukKyQ5&#TzX^)mg5%JzDuZKuA*xi(uh5s)%zZ!2CCY&(@5T',
   minConnections: 1,
   maxConnections: 16
})
```

### Query execution

Through a connection pool, queries can be `run()`. Queries with no dynamic parts on them (save their parameters) should be defined using the `sql` template literal tag. This allows for static type checking later on, such that the types of the columns returned by the query are taken into account at compile time, just like for any regular TypeScript function. For this reason, it is important that `sql` is not aliased, and that no other template literal tag named the same exists elsewhere in the codebase.

To prevent SQL injection, template literal placeholders are replaced with Postgres query placeholders, and the query is prepared and executed in separate steps.

```ts
import { sql } from 'pgeon/postgres-client'
import { db } from './db'

const name = 'john'

db
   .run(sql`select * from people where name = ${name}`)
   .then(queryResult => console.debug(queryResult))
```

The same method accepts dynamic queries too. Parameter SQL injection is prevented in the same way as for static queries.

```ts
import { db } from './db'

const dynamicCriteria = true ? 'name = $1' : 'nickname = $1'

const name = 'john'

db
   .run({
      sql: `select * from people where ${dynamicCriteria}`,
      params: [name]
   })
   .then(queryResult => console.debug(queryResult))
```


## webpack loader

In order to enable compile-time checks of static SQL queries, the [webpack loader](./webpack-loader.ts) must be run _before_ your TypeScript loader of choice. In webpack, this means placing it _after_ said TypeScript loader in the webpack configuration.

Note that in order to write the webpack configuration in TypeScript, as well as to be able to reference loaders written in TypeScript directly, [ts-node](https://github.com/TypeStrong/ts-node) is needed as a dependency.

```ts
import { Configuration } from 'webpack'

const webpackConfig: Configuration = {
   entry: './main.ts',
   target: 'node',
   module: {
      rules: [{
         test: /\.ts$/,
         use: ['ts-loader', 'pgeon/webpack-loader.ts']
      }]
   },
   resolve: {
      extensions: ['.ts']
   }
}

export default webpackConfig
```

As an example, the following code fails at compile time due to the type mismatch it introduces.

```ts
import { newPool, sql } from 'pgeon/postgres-client'

const db = newPool()

db
   .run(sql`select 1 as number`)
   .then(queryResult => {
      const one: string = queryResult.rows[0].number
      console.debug(one)
   })
```

TypeScript will emit a regular error, as illustrated below. Notice that all returned values are assumed to be nullable, unless they are known to refer to a non-nullable database column, as Postgres does not offer better information in this regard.

```
ERROR in /example.ts(8,11)
  TS2322: Type 'number | null' is not assignable to type 'string'.
```
