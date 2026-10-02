"""Prepare an isolated local test manifest; never publish its credentials."""
from pathlib import Path
import json
import secrets
import os

root = Path(__file__).resolve().parents[1]
lab = Path(os.environ.get('SPARK_PUSH_LAB_DIR', '/tmp/spark-im-lab')).resolve()
lab.mkdir(exist_ok=True, parents=True, mode=0o700)
os.umask(0o077)
password = secrets.token_hex(24)
(lab / 'db.env').write_text('SPARK_PUSH_MYSQL_PASSWORD=' + password + '\n')
schema = (root / 'sql/schema.sql').read_text().replace('`spark_push`', '`spark_im_test_20261002`')
(lab / 'init.sql').write_text(schema + "\nCREATE USER IF NOT EXISTS 'spark_im_test'@'127.0.0.1' IDENTIFIED BY '" + password + "';\nALTER USER 'spark_im_test'@'127.0.0.1' IDENTIFIED BY '" + password + "';\nGRANT ALL ON spark_im_test_20261002.* TO 'spark_im_test'@'127.0.0.1';\n")
shared = dict(kafka_brokers='127.0.0.1:19092', mysql_host='127.0.0.1', mysql_port='3306',
    mysql_user='spark_im_test', mysql_password=password, mysql_db='spark_im_test_20261002',
    redis_host='127.0.0.1', redis_port='6379', redis_db='14', redis_password='',
    redis_pool_size='16', redis_max_pool_size='32', kafka_consumer_group='im_reliability_lab',
    comet_targets='comet-lab=127.0.0.1:19105', attachment_dir=str(lab/'attachments'),
    single_rate_per_sec='100000', single_burst='100000', group_rate_per_sec='100000', group_burst='100000')
for kind in ['single','group','push','broadcast','persist','ai_request','ai_delta','ai_reply']:
    shared['kafka_' + kind + '_topic'] = 'imlab_' + kind
for name, opts in dict(logic=dict(listen_addr='127.0.0.1',listen_port='19100',http_port='19101',hermes_enabled='false'),
    comet=dict(listen_addr='127.0.0.1',listen_port='19000',comet_id='comet-lab',comet_grpc_port='19105',metrics_port='19203',logic_grpc_target='127.0.0.1:19100'),
    job=dict(metrics_port='19202')).items():
    values = {}
    for line in (root / 'conf' / (name+'.conf')).read_text().splitlines():
        if line.strip() and not line.startswith('#') and '=' in line:
            k,v=line.split('=',1);values[k]=v
    values.update(shared);values.update(opts)
    (lab/(name+'.conf')).write_text('\n'.join(k+'='+v for k,v in values.items())+'\n')
variants={}
for label,build in dict(baseline='/home/peco/cppcode/grpc_aiagent/build-wsl',updated=os.environ.get('SPARK_PUSH_BUILD_DIR','/tmp/spark-im-build')).items():
    variants[label]=dict(build=build,logic_config=str(lab/'logic.conf'),job_config=str(lab/'job.conf'),
        comet_config=str(lab/'comet.conf'),http_port=19101,ws_port=19000,grpc_port=19100)
(lab/'manifest.json').write_text(json.dumps(dict(variants=variants,
    mysql=dict(database=shared['mysql_db'],host='127.0.0.1',port=3306,user=shared['mysql_user'],password=password),
    redis=dict(port=6379,database=14))))
(lab/'kafka.properties').write_text('''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19093
listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://127.0.0.1:19092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=LAB_DIR/kafka-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.partitions=8
group.initial.rebalance.delay.ms=0
'''.replace('LAB_DIR',str(lab)))
print('Prepared isolated configs and manifest; credentials stay in '+str(lab))

# Each measured variant owns its database, topics and consumer offsets.
# This avoids cold replay of another variant's backlog contaminating latency.
manifest = json.loads((lab/'manifest.json').read_text())
extra_schema = ''
for label in ['baseline', 'updated']:
    database = 'spark_im_' + label + '_20261002'
    extra_schema += schema.replace('spark_im_test_20261002', database)
    extra_schema += "\nGRANT ALL ON " + database + ".* TO 'spark_im_test'@'127.0.0.1';\n"
    for component in ['logic','job','comet']:
        config = (lab/(component+'.conf')).read_text()
        config = config.replace('mysql_db=spark_im_test_20261002','mysql_db='+database)
        config = config.replace('imlab_', 'imlab_'+label+'_')
        config = config.replace('kafka_consumer_group=im_reliability_lab', 'kafka_consumer_group=im_reliability_lab_'+label)
        path = lab/(label+'-'+component+'.conf');path.write_text(config)
        manifest['variants'][label][component+'_config'] = str(path)
manifest['mysql']['database'] = 'spark_im_updated_20261002'
(lab/'manifest.json').write_text(json.dumps(manifest))
(lab/'init.sql').write_text((lab/'init.sql').read_text()+extra_schema+
    schema.replace('spark_im_test_20261002','spark_im_dao_test_20261002')+
    "\nGRANT ALL ON spark_im_dao_test_20261002.* TO 'spark_im_test'@'127.0.0.1';\n")
