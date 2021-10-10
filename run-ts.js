#!/usr/bin/env node

const { execSync } = require('child_process')

const [inputScript] = process.argv.slice(2)
const outputScript = inputScript.slice(0, -3) + '.js'

console.log('Compiling...')
execSync(`./node_modules/.bin/tsc --target ESNext --module CommonJS --outDir /tmp ${inputScript}`, { stdio: 'inherit' })

console.log('Running...')
execSync(`node /tmp/${outputScript}`, { stdio: 'inherit' })
