import { readFileSync } from 'fs'
import { spawn, spawnSync } from 'child_process'
import { newPool } from './src/pool'
import { getSourceWithQueryTypes } from './src/typing'

const args = process.argv.slice(2)
const isWatchMode = args.includes('-w') || args.includes('--watch')
const tscCommand = './node_modules/.bin/tsc'

const pool = newPool()
// TODO pool.destroy()

if (isWatchMode) {
   const tscWatchProcess = spawn(tscCommand, ['--watch', '--pretty', '--listFilesOnly', ...args])
   tscWatchProcess.stdin.pipe(process.stdin)
   tscWatchProcess.stdout.pipe(process.stdout)
   tscWatchProcess.stderr.pipe(process.stderr)

   const tscSuccessMessage = 'Found 0 errors. Watching for file changes.'
   tscWatchProcess.stdout.on('data', (output: Buffer) => {
      console.log('output', output.toString())

      if (output.includes(tscSuccessMessage)) {
         // TODO Run static analysis.
      }
   })
} else {
   const { stdout } = spawnSync(tscCommand, ['--listFilesOnly'])
   const tscFiles = stdout
      .toString()
      .trim()
      .split('\n')
      .filter(file => !file.includes('/node_modules/'))
      .filter(file => !file.endsWith('.d.ts'))
      .filter(file => file.endsWith('.ts'))

   for (const file of tscFiles) {
      console.log(file)
      const source = readFileSync(file).toString()
      getSourceWithQueryTypes(pool, source).then(console.log)
   }
}
