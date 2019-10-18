#!/usr/bin/env node

import {ok, deepStrictEqual as eq} from 'assert'
import {execSync} from 'child_process'

const scan = (file: string) => execSync(`node ./bin/cli.js scan ${file}`).toString()

let output: string

output = scan('tests/syntax-error.ts')
ok(output.includes('tests/syntax-error.ts'))
ok(output.includes('syntax error at or near "elect"'))
ok(/ e.*l.*ect one/.test(output))

output = scan('tests/unknown-table-select.ts')
ok(output.includes('tests/unknown-table-select.ts'))
ok(output.includes('relation "users" does not exist'))
ok(output.includes(' select *'))

output = scan('tests/unknown-table-insert.ts')
ok(output.includes('tests/unknown-table-insert.ts'))
ok(output.includes('relation "t" does not exist'))
ok(output.includes(' insert into '))

output = scan('tests/unknown-table-update.ts')
ok(output.includes('tests/unknown-table-update.ts'))
ok(output.includes('relation "t" does not exist'))
ok(output.includes(' update '))

output = scan('tests/unknown-table-delete.ts')
ok(output.includes('tests/unknown-table-delete.ts'))
ok(output.includes('relation "t" does not exist'))
ok(output.includes(' delete from '))

output = scan('tests/type-mismatch.ts')
ok(output.includes('tests/type-mismatch.ts'))
ok(output.includes('type mismatch in "Row.a_number"'))
ok(output.includes(' select 1'))

output = scan('tests/no-placeholders.ts')
eq(output, '')

output = scan('tests/nested-placeholders.ts')
eq(output, '')

output = scan('tests/all-types.ts')
eq(output, '')

output = scan('tests/with-query-options.ts')
eq(output, '')

output = scan('tests/update.ts')
eq(output, '')
