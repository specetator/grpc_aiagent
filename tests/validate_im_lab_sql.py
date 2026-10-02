"""Read-only validation before applying the generated isolated lab schema."""
import hashlib
import re
import sys
from pathlib import Path

path=Path(sys.argv[1]);raw=path.read_text()
sql=re.sub(r'--[^\n]*','',raw)
statements=[s.strip() for s in sql.split(';') if s.strip()]
allowed_databases={'spark_im_test_20261002','spark_im_baseline_20261002',
    'spark_im_updated_20261002','spark_im_dao_test_20261002'}
databases=set()
for statement in statements:
    if statement.startswith('CREATE TABLE IF NOT EXISTS '):
        assert re.match(r'CREATE TABLE IF NOT EXISTS [`A-Za-z_]',statement)
    elif statement.startswith(('CREATE DATABASE IF NOT EXISTS ', 'USE ')):
        name=re.search(r'(?:CREATE DATABASE IF NOT EXISTS|USE)\s+`?([A-Za-z0-9_]+)',statement)[1]
        assert name in allowed_databases,name
        databases.add(name)
    elif statement.startswith(('CREATE USER IF NOT EXISTS ', 'ALTER USER ')):
        assert re.match(r"(?:CREATE USER IF NOT EXISTS|ALTER USER) 'spark_im_test'@'127.0.0.1' IDENTIFIED BY '[a-f0-9]{48}'$",statement)
    elif statement.startswith('GRANT ALL ON '):
        match=re.fullmatch(r"GRANT ALL ON ([a-z0-9_]+)\.\* TO 'spark_im_test'@'127.0.0.1'",statement)
        assert match and match[1] in allowed_databases
    else:
        raise ValueError('Unexpected SQL statement type: '+statement.split()[0])
assert databases==allowed_databases
print('Validated isolated databases:',', '.join(sorted(databases)))
print('Only additive tables/schema and the loopback test user; no destructive statements')
print('Statements:',len(statements),'SHA256:',hashlib.sha256(raw.encode()).hexdigest())
