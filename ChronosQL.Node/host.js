const { spawnSync } = require('child_process');
const path = require('path');

const sql = 'SELECT data.value FROM input';
const input = '{"data":{"value":1}}\n{"data":{"value":2}}';

const projectPath = path.join(__dirname, 'ChronosQL.Node.csproj');
const result = spawnSync('dotnet', ['run', '--project', projectPath, '--', sql, input], {
  encoding: 'utf8'
});

if (result.error) {
  console.error(result.error);
  process.exit(1);
}

if (result.stderr) {
  console.error(result.stderr.trim());
}

console.log(result.stdout.trim());
