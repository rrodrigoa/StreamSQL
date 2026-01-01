import os
import subprocess
import sys

sql = 'SELECT data.value FROM input'
input_payload = '{"data":{"value":1}}\n{"data":{"value":2}}'
project_path = os.path.join(os.path.dirname(__file__), 'ChronosQL.Python.csproj')

result = subprocess.run(
    ['dotnet', 'run', '--project', project_path, '--', sql, input_payload],
    capture_output=True,
    text=True,
)

if result.returncode != 0:
    sys.stderr.write(result.stderr)
    sys.exit(result.returncode)

if result.stderr:
    sys.stderr.write(result.stderr)

print(result.stdout.strip())
