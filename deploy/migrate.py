"""Serialized, restartable MySQL upgrades for the single-host distribution.

Historical SQL is retained for manual deployments. We inspect older layouts
before creating current tables; notably, never replay the cursor reset on a
database which already uses durable message reservations.
"""
import hashlib
import json
import os
from pathlib import Path
import sys
import pymysql
from pymysql.constants import CLIENT

SQL = Path(__file__).resolve().parents[1] / 'sql'

class Migrator:
    def __init__(self, connection):
        self.db = connection

    def execute(self, sql, parameters=None):
        with self.db.cursor() as cursor:
            cursor.execute(sql, parameters)
            rows = cursor.fetchall() if cursor.description else ()
            while cursor.nextset():
                pass
            return rows

    def table(self, name):
        return bool(self.execute('SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s', ('spark_push', name)))

    def column(self, table, name):
        return bool(self.execute('SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s', ('spark_push', table, name)))

    def validate_allocator(self):
        expected = {
            'message_sequence': {'session_id':'varchar','last_seq':'bigint'},
            'message_reservation': {'session_id':'varchar','msg_seq':'bigint','sender_id':'bigint',
                'client_msg_id':'varbinary','original_message':'mediumblob','persist_payload':'mediumblob',
                'stage':'tinyint','created_at':'datetime'},
            'message_delivery_receipt': {'user_id':'bigint','session_id':'varchar','msg_seq':'bigint'}
        }
        keys = {'message_sequence': [('session_id',)],
                'message_reservation': [('session_id','msg_seq'),('session_id','sender_id','client_msg_id')],
                'message_delivery_receipt': [('user_id','session_id','msg_seq')]}
        for table, fields in expected.items():
            rows = self.execute('SELECT COLUMN_NAME,DATA_TYPE,COLLATION_NAME,IS_NULLABLE,CHARACTER_MAXIMUM_LENGTH,COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s', ('spark_push',table))
            columns = {row[0]:row[1:] for row in rows}
            engine = self.execute('SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s', ('spark_push',table))
            if (not engine or engine[0][0]!='InnoDB' or
                any(name not in columns or columns[name][0]!=kind or
                    columns[name][2]!=('YES' if name=='persist_payload' else 'NO') or
                    'unsigned' in columns[name][4] for name,kind in fields.items()) or
                columns['session_id'][1]!='ascii_bin' or columns['session_id'][3]!=128 or
                ('client_msg_id' in columns and columns['client_msg_id'][3]!=128)):
                raise RuntimeError('Unsupported or damaged durable allocator table: '+table)
            indexes = self.execute('SELECT INDEX_NAME,COLUMN_NAME,SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND NON_UNIQUE=0 ORDER BY INDEX_NAME,SEQ_IN_INDEX', ('spark_push',table))
            unique = {}
            for name,column,prefix in indexes:unique.setdefault(name,[]).append(column if prefix is None else column+'(prefix)')
            if tuple(unique.get('PRIMARY',[]))!=keys[table][0] or any(key not in [tuple(value) for value in unique.values()] for key in keys[table]):
                raise RuntimeError('Missing durable allocator uniqueness constraint: '+table)

    def apply(self, path):
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        applied = self.execute('SELECT checksum FROM spark_schema_migration WHERE version=%s', (path.name,))
        if applied:
            if applied[0][0] != checksum:
                raise RuntimeError('Applied migration checksum differs: ' + path.name)
            return
        self.execute(path.read_text())
        self.execute('INSERT INTO spark_schema_migration(version,checksum) VALUES(%s,%s)', (path.name, checksum))
        print('Applied ' + path.name, flush=True)

    def run(self, app_password):
        if self.execute("SELECT GET_LOCK('sparkpush-schema', 60)")[0][0] != 1:
            raise RuntimeError('Another migration is running; retry after it finishes.')
        try:
            existed = self.table('user')
            allocator = self.table('message_sequence') and self.table('message_reservation')
            state = self.execute('SELECT legacy_pending FROM spark_push.spark_schema_state WHERE id=1') if self.table('spark_schema_state') else ()
            pending_legacy = bool(state and state[0][0])
            if (pending_legacy or (existed and not allocator)) and os.environ.get('SPARK_LEGACY_KAFKA_DRAINED') != 'true':
                raise RuntimeError('Legacy database lacks the durable allocator. Stop old writers, drain Kafka, back up data, then set SPARK_LEGACY_KAFKA_DRAINED=true for this upgrade.')
            if allocator and not pending_legacy:
                self.validate_allocator()
            if self.table('spark_schema_migration'):
                for version, checksum in self.execute('SELECT version,checksum FROM spark_push.spark_schema_migration'):
                    path = SQL/'migrations'/version
                    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != checksum:
                        raise RuntimeError('Applied migration checksum differs: ' + version)
            self.execute('CREATE DATABASE IF NOT EXISTS spark_push CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; USE spark_push')
            self.execute('CREATE TABLE IF NOT EXISTS spark_schema_state (id TINYINT PRIMARY KEY, legacy_pending BOOLEAN NOT NULL) ENGINE=InnoDB')
            self.execute('INSERT INTO spark_schema_state(id,legacy_pending) VALUES(1,%s) ON DUPLICATE KEY UPDATE legacy_pending=(legacy_pending OR VALUES(legacy_pending))', (bool(existed and not allocator),))
            pending_legacy = bool(self.execute('SELECT legacy_pending FROM spark_schema_state WHERE id=1')[0][0])
            # CREATE IF NOT EXISTS is safe after the preceding compatibility gate.
            self.execute((SQL/'schema.sql').read_text())
            self.execute('''CREATE TABLE IF NOT EXISTS spark_schema_migration (
                version VARCHAR(160) CHARACTER SET ascii PRIMARY KEY,
                checksum CHAR(64) CHARACTER SET ascii NOT NULL,
                applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB''')
            # The earliest ALTER scripts were not rerunnable on MySQL 8. Inspect
            # postconditions instead; every step is safe after a partial DDL run.
            for table, name, definition in (
                ('user', 'status', 'TINYINT NOT NULL DEFAULT 1'),
                ('user', 'deleted_at', 'DATETIME(3) NULL DEFAULT NULL'),
                ('user', 'updated_at', 'DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)'),
                ('user_session_state', 'delivered_seq', 'BIGINT NOT NULL DEFAULT 0')):
                if not self.column(table, name):
                    self.execute(f'ALTER TABLE `{table}` ADD COLUMN `{name}` {definition}')
            if not self.execute("SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='spark_push' AND TABLE_NAME='user' AND INDEX_NAME='idx_user_status'"):
                self.execute('CREATE INDEX idx_user_status ON user(status,deleted_at)')
            content_type = self.execute("SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='spark_push' AND TABLE_NAME='message' AND COLUMN_NAME='content_json'")[0][0]
            if content_type == 'text':
                self.execute('ALTER TABLE message MODIFY COLUMN content_json MEDIUMTEXT NOT NULL')
            if pending_legacy:
                # No legacy MAX cursor proves a continuous delivered prefix.
                self.execute('UPDATE user_session_state SET delivered_seq=0')
                self.execute('UPDATE spark_schema_state SET legacy_pending=FALSE WHERE id=1')
            for path in sorted((SQL/'migrations').glob('20*.sql')):
                if path.name == '20261011_message_reservations.sql':
                    continue  # Baseline adoption above never repeats its reset.
                self.apply(path)
            for table, columns in {
                'message_sequence': ('session_id','last_seq'),
                'message_reservation': ('session_id','msg_seq','sender_id','client_msg_id','original_message','persist_payload','stage'),
                'message_delivery_receipt': ('user_id','session_id','msg_seq'),
                'im_group': ('description','visibility'),
                'room_agent_dispatch': ('request_id','attempted'),
                'room_collaboration_task': ('task_id','revision','payload')
            }.items():
                if not all(self.column(table, col) for col in columns):
                    raise RuntimeError('Unsupported incomplete table layout: ' + table)
            self.validate_allocator()
            self.execute("CREATE USER IF NOT EXISTS 'spark'@'%%' IDENTIFIED BY %s", (app_password,))
            self.execute("ALTER USER 'spark'@'%%' IDENTIFIED BY %s", (app_password,))
            # Existing services perform idempotent schema checks at startup.
            self.execute("GRANT ALL PRIVILEGES ON spark_push.* TO 'spark'@'%'")
            print('Database ready; historical messages, tasks and current cursors retained.', flush=True)
        finally:
            self.execute("SELECT RELEASE_LOCK('sparkpush-schema')")

def main():
    security = json.loads(Path('/run/spark-secrets/security.json').read_text())
    root_password = Path('/run/spark-admin/mysql-root-password').read_text()
    connection = pymysql.connect(host=os.environ.get('SPARK_MYSQL_HOST','mysql'), port=int(os.environ.get('SPARK_MYSQL_PORT','3306')),
        user='root', password=root_password, charset='utf8mb4', autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS, connect_timeout=10, read_timeout=120)
    try:
        Migrator(connection).run(security['mysql'])
    finally:
        connection.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('Migration failed: ' + str(error), file=sys.stderr)
        sys.exit(1)
