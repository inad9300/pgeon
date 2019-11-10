#!/usr/bin/env node

import {ok, deepStrictEqual as eq} from 'assert'
import {execSync} from 'child_process'

const scan = (file: string) => execSync(`node ./bin/cli.js scan fixtures/${file}`).toString()

let output: string

output = scan('syntax-error.ts')
ok(output.includes('syntax-error.ts'))
ok(output.includes('syntax error at or near "elect"'))
ok(/ e.*l.*ect one/.test(output))

output = scan('unknown-table-select.ts')
ok(output.includes('unknown-table-select.ts'))
ok(output.includes('relation "users" does not exist'))
ok(output.includes(' select *'))

output = scan('unknown-table-insert.ts')
ok(output.includes('unknown-table-insert.ts'))
ok(output.includes('relation "t" does not exist'))
ok(output.includes(' insert into '))

output = scan('unknown-table-update.ts')
ok(output.includes('unknown-table-update.ts'))
ok(output.includes('relation "t" does not exist'))
ok(output.includes(' update '))

output = scan('unknown-table-delete.ts')
ok(output.includes('unknown-table-delete.ts'))
ok(output.includes('relation "t" does not exist'))
ok(output.includes(' delete from '))

output = scan('type-mismatch.ts')
ok(output.includes('type-mismatch.ts'))
ok(output.includes('type mismatch in "Row.a_number"'))
ok(output.includes(' select 1'))

output = scan('no-placeholders.ts')
eq(output, '')

output = scan('nested-placeholders.ts')
eq(output, '')

output = scan('all-types.ts')
eq(output, '')

output = scan('with-query-options.ts')
eq(output, '')

output = scan('update.ts')
eq(output, '')

output = scan('intersection-type.ts')
eq(output, '')
