#!/usr/bin/env python3
"""Layer-boundary guard, complementary to behaviour tests (not an AST proof)."""
import re
from pathlib import Path

root = Path(__file__).resolve().parents[1]
for relative in ('logic/http_server.h', 'logic/http_server.cpp',
                 'logic/grpc_service.h', 'logic/grpc_service.cpp'):
    source = (root / relative).read_text()
    assert not re.search(r'#include\s+"(?:[^"\n]*_dao|redis_store|kafka_producer|conversation_store)\.h"', source), relative
    assert not re.search(r'\b(?:UserDao|AttachmentDao|GroupDao|GroupMemberDao|RedisStore|KafkaProducer|ConversationStore)\s*\*', source), relative
    assert not re.search(r'\b(?:user_dao_|group_member_dao_|redis_store_|persist_producer_|store_)->', source), relative
for path in (root / 'logic/services').glob('*.h'):
    source = path.read_text()
    assert not re.search(r'#include\s+[<"](?:grpcpp|muduo|spark_push\.pb|spark_push\.grpc)', source), path.name
    assert not re.search(r'\b(?:HttpRequest|HttpResponse|ServerContext|UpstreamMessageRequest|UpstreamMessageReply)\b', source), path.name
main = (root / 'logic/main.cpp').read_text()
assert 'message_service.HandleHermesReply' in main and 'message_service.HandleHermesDelta' in main
assert 'service->HandleHermesReply' not in main
print('logic layer contracts passed: adapters depend on Services, business DTOs do not depend on HTTP/gRPC')
