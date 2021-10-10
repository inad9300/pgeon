import { execSync } from 'child_process'

function dockerRun(label: string, args: string, command = '') {
   const imageName = `pgeon_${label}_image`
   const containerName = `pgeon_${label}_container`

   execSync(`docker container rm --force ${containerName}`, { stdio: 'ignore' })
   execSync(`docker image rm --force ${imageName}`, { stdio: 'ignore' })
   execSync(`docker build --tag ${imageName} --file Dockerfile.${label} .`, { stdio: 'ignore' })
   execSync(`docker run --rm --detach --name ${containerName} ${args} ${imageName} ${command}`, { stdio: 'ignore' })

   return {
      getIp: () => execSync(`docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${containerName}`).toString().trim(),
      follow: () => execSync(`docker logs --follow ${containerName}`, { stdio: 'inherit' })
   }
}

const PGPASSWORD = 'jB8tBAotpE2Z89yjYsJe6u6jNCfHnzxY'
const PGHOST = dockerRun('postgres', `--env POSTGRES_PASSWORD=${PGPASSWORD}`).getIp()

dockerRun(
   'node',
   `--env PGPASSWORD=${PGPASSWORD} --env PGHOST=${PGHOST}`,
   `sh -c './node_modules/.bin/tsc && node out/src/pool.test.js'`
)
.follow()
