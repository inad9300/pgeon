#!/usr/bin/env node

import {ok, deepStrictEqual as eq} from 'assert'
import {execSync} from 'child_process'

const scan = (file: string) => execSync(`node ./bin/cli.js scan ${file}`).toString()

/*
create table users (
    id serial primary key,
    name varchar(50) not null,
    gender char(1) not null,
    is_human bool not null,
    descents smallint not null,
    joined_year int,
    points float not null,
    birthday date not null,
    picture bytea not null
);
*/

let output: string

output = scan('tests/syntax-error.ts')
ok(output.includes('tests/syntax-error.ts'))
ok(output.includes('syntax error at or near "from"'))
ok(output.includes('elect one'))

output = scan('tests/unknown-table.ts')
ok(output.includes('tests/unknown-table.ts'))
ok(output.includes('relation "users" does not exist'))
ok(output.includes('select *'))

output = scan('tests/type-mismatch.ts')
ok(output.includes('tests/type-mismatch.ts'))
ok(output.includes('type mismatch in "Row.a_number"'))
ok(output.includes('select 1'))

output = scan('tests/no-placeholders.ts')
eq(output, '')

output = scan('tests/nested-placeholders.ts')
eq(output, '')

output = scan('tests/all-types.ts')
eq(output, '')

output = scan('tests/with-query-options.ts')
eq(output, '')
