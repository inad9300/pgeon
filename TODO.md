- Type-check input values ("placeholders").
- Support types such as `true`, `1`, `1 | 2`...
- Check out supported types and JavaScript-to-Postgres type mapping in [pg-types](https://github.com/brianc/node-pg-types).
- Complete type definitions for `pg-query-native` (see https://github.com/lfittl/libpg_query/issues/51).
- Think about SQL statements other than the basic CRUD.
- Check nullability when possible (columns in `select` statements, `returning` in `insert`/`update`/`delete` statements).
  + https://www.postgresql.org/docs/current/infoschema-columns.html
  + https://www.postgresql.org/docs/current/infoschema-attributes.html
