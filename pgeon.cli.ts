#!/usr/bin/env node

import * as pgeon from './pgeon.lib'

const help = `
pgeon – Type checker for PostgreSQL queries written in TypeScript and node-posgres.

··· Commands ···

pgeon scan [<dir>] – Scan *.ts and *.tsx files in the given directory (or, by default, the current working directory) for type errors.
`

;(async () => {
    try {
        const args = process.argv.slice(2)
        switch (args[0]) {
            case 'help': {
                console.log(help)
                process.exit(0)
                break
            }
            case 'scan': {
                const dir = args[1] || process.cwd()
                await pgeon.scanFiles(
                    pgeon.filesEndingWith(dir, ['.ts', '.tsx'], ['.d.ts'])
                )

                process.exit(0)
                break
            }
            default: {
                console.error(`Unsupported command: "${args[0]}". Try running \`pgeon help\`.`)
                process.exit(1)
                break
            }
        }
    } catch (err) {
        console.error(`Unexpected error: ${err.message}`)
        process.exit(1)
    }
})()
