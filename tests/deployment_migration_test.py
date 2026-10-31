#!/usr/bin/env python3
"""Real isolated MySQL migration regression; never touches a personal database."""
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
import threading
import pymysql
from pymysql.constants import CLIENT

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('spark_migrate', ROOT/'deploy/migrate.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def main():
    name = 'spark-migration-' + secrets.token_hex(6)
    password = secrets.token_hex(24)
    connection = None
    checks = []
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    environment = dict(os.environ, MYSQL_ROOT_PASSWORD=password)
    subprocess.run(['docker','run','-d','--rm','--name',name,'-e','MYSQL_ROOT_PASSWORD','-e','MYSQL_ROOT_HOST=%',
                    '-p',f'127.0.0.1:{port}:3306','mysql:8.0'], env=environment, check=True, stdout=subprocess.DEVNULL)
    def expect(value, description):
        if not value:
            raise AssertionError(description)
        checks.append(description)
    try:
        deadline = time.monotonic()+120
        while time.monotonic()<deadline:
            try:
                connection=pymysql.connect(host='127.0.0.1', port=port,user='root',password=password,
                    charset='utf8mb4', autocommit=True,client_flag=CLIENT.MULTI_STATEMENTS)
                break
            except pymysql.MySQLError:
                time.sleep(.5)
        if connection is None:
            raise AssertionError('MySQL readiness timeout')
        migration=module.Migrator(connection)
        sql=migration.execute
        migration.run('app-test-'+secrets.token_hex(20))
        expect(migration.table('room_collaboration_task') and migration.column('im_group','visibility'),'fresh schema includes spaces and tasks')
        sql("INSERT INTO user(account,name,password_hash) VALUES('migration','Migration','test-only')")
        sql("INSERT INTO user_session_state(user_id,session_id,read_seq,delivered_seq) VALUES(1,'r_1',23,17)")
        sql("INSERT INTO room_agent_task(room_id,revision,payload) VALUES(1,2,'{\"workspace\":\"kept\"}')")
        before=sql('SELECT version,checksum FROM spark_schema_migration ORDER BY version')
        migration.run('app-test-'+secrets.token_hex(20))
        expect(sql('SELECT delivered_seq FROM user_session_state')[0][0]==17,'rerun preserves delivered cursor')
        expect(sql('SELECT revision,payload FROM room_agent_task')[0][0]==2,'rerun preserves CANN task')
        expect(before==sql('SELECT version,checksum FROM spark_schema_migration ORDER BY version'),'rerun does not duplicate migration ledger')
        sql('ALTER TABLE message_reservation DROP INDEX dedup')
        try:migration.run('app-test')
        except RuntimeError as error:expect('uniqueness' in str(error),'damaged allocator uniqueness is rejected before automatic upgrade')
        else:raise AssertionError('allocator without idempotency key was accepted')
        sql('ALTER TABLE message_reservation ADD UNIQUE KEY dedup(session_id,sender_id,client_msg_id)')
        sql('ALTER TABLE message_reservation DROP PRIMARY KEY, ADD PRIMARY KEY(session_id,msg_seq,sender_id)')
        try:migration.run('app-test')
        except RuntimeError as error:expect('uniqueness' in str(error),'incorrect allocator primary key is rejected')
        else:raise AssertionError('wrong allocator primary key was accepted')
        expect(before==sql('SELECT version,checksum FROM spark_schema_migration ORDER BY version'),'rejected allocator does not change the migration ledger')
        sql('ALTER TABLE message_reservation DROP PRIMARY KEY, ADD PRIMARY KEY(session_id,msg_seq)')
        sql('ALTER TABLE message_reservation MODIFY sender_id BIGINT NULL')
        try:migration.run('app-test')
        except RuntimeError as error:expect('damaged' in str(error),'nullable idempotency identity is rejected')
        else:raise AssertionError('nullable idempotency identity was accepted')
        sql('ALTER TABLE message_reservation MODIFY sender_id BIGINT NOT NULL')
        # Existing current main deployment without a ledger: adopt current
        # reservations while adding new room schema, never reset cursors.
        sql('DROP TABLE spark_schema_migration; DROP TABLE room_agent_binding; DROP TABLE room_agent_dispatch; DROP TABLE room_collaboration_task; DROP TABLE room_collaboration_task_history')
        migration.run('app-test-'+secrets.token_hex(20))
        expect(migration.table('room_agent_binding') and sql('SELECT delivered_seq FROM user_session_state')[0][0]==17,'current main upgrade preserves cursor and adds missing tables')
        version,checksum=before[0]
        sql('UPDATE spark_schema_migration SET checksum=%s WHERE version=%s',('0'*64,version))
        try: migration.run('app-test')
        except RuntimeError as error: expect('checksum' in str(error),'checksum mismatch stops migration')
        else: raise AssertionError('tampered migration accepted')
        sql('UPDATE spark_schema_migration SET checksum=%s WHERE version=%s',(checksum,version))
        # A different connection owns GET_LOCK, proving DDL cannot race.
        other=pymysql.connect(host='127.0.0.1',port=port,user='root',password=password,autocommit=True)
        try:
            with other.cursor() as cursor:
                cursor.execute("SELECT GET_LOCK('sparkpush-schema',1)")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                started=threading.Event()
                def migrate_while_locked():
                    started.set()
                    migration.run('app-test-'+secrets.token_hex(20))
                future=pool.submit(migrate_while_locked)
                started.wait(5)
                time.sleep(.3)
                expect(not future.done(),'second migration waits for the database lock')
                with other.cursor() as cursor:
                    cursor.execute("SELECT RELEASE_LOCK('sparkpush-schema')")
                future.result(timeout=10)
                expect(True,'waiting migration completes after the owning connection releases the lock')
        finally: other.close()
        interrupted='20261031_generic_room_agents.sql'
        sql('DELETE FROM spark_schema_migration WHERE version=%s',(interrupted,))
        execute=migration.execute
        def fail_before_receipt(statement, parameters=None):
            if statement.startswith('INSERT INTO spark_schema_migration') and parameters[0]==interrupted:
                raise RuntimeError('injected interruption after DDL, before migration receipt')
            return execute(statement,parameters)
        migration.execute=fail_before_receipt
        try: migration.run('app-test')
        except RuntimeError: pass
        else: raise AssertionError('expected injected migration interruption')
        migration.execute=execute
        expect(not sql('SELECT 1 FROM spark_schema_migration WHERE version=%s',(interrupted,)),'failed migration does not record success')
        migration.run('app-test-'+secrets.token_hex(20))
        expect(bool(sql('SELECT 1 FROM spark_schema_migration WHERE version=%s',(interrupted,))),'DDL interruption resumes safely with the same migration')
        # Simulate an older installation, including a partially completed
        # allocator DDL. Refuse before adding missing tables without opt-in.
        sql('DROP TABLE message_reservation; DROP TABLE message_sequence; DROP TABLE room_agent_dispatch')
        try: migration.run('app-test')
        except RuntimeError as error: expect('drain Kafka' in str(error),'old allocator upgrade requires drained confirmation')
        else: raise AssertionError('legacy upgrade was accepted without confirmation')
        expect(not migration.table('room_agent_dispatch'),'refusal does not initialize other tables')
        os.environ['SPARK_LEGACY_KAFKA_DRAINED']='true'
        migration.run('app-test-'+secrets.token_hex(20))
        expect(sql('SELECT delivered_seq FROM user_session_state')[0][0]==0,'explicit old allocator migration resets only the unproven legacy prefix')
        sql('UPDATE user_session_state SET delivered_seq=9')
        migration.run('app-test-'+secrets.token_hex(20))
        expect(sql('SELECT delivered_seq FROM user_session_state')[0][0]==9,'completed legacy migration never resets the cursor again')
        print(json.dumps({'ok':True,'mode':'real isolated MySQL 8','checks':checks},ensure_ascii=False))
    finally:
        if connection: connection.close()
        subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

if __name__=='__main__':main()
