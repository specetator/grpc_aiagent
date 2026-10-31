# syntax=docker/dockerfile:1
FROM ubuntu:22.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config ca-certificates libssl-dev libcurl4-openssl-dev \
    default-libmysqlclient-dev libhiredis-dev librdkafka-dev libprotobuf-dev \
    protobuf-compiler protobuf-compiler-grpc libgrpc++-dev nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt .
COPY cmake/ cmake/
COPY muduo/ muduo/
COPY third_party/json.hpp third_party/json.hpp
COPY common/ common/
COPY proto/ proto/
COPY logic/ logic/
COPY comet/ comet/
COPY job/ job/
COPY hermes_bridge/ hermes_bridge/
COPY inference/ inference/
COPY load_test/ load_test/
COPY tools/ tools/
COPY web_demo/CMakeLists.txt web_demo/web_server.cpp web_demo/
ARG BUILD_JOBS=2
RUN cmake -S . -B /build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF \
    && cmake --build /build --parallel ${BUILD_JOBS} --target \
       logic_server comet_server job_server web_demo_server hermes_bridge

FROM ubuntu:22.04 AS runtime
ENV DEBIAN_FRONTEND=noninteractive SPARK_LOG_CONSOLE=1 PYTHONDONTWRITEBYTECODE=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl openssl python3 python3-pymysql python3-cryptography libssl3 libcurl4 \
    libmysqlclient21 libhiredis0.14 librdkafka++1 libprotobuf23 libgrpc++1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 10001 spark && useradd -u 10001 -g spark -m spark
WORKDIR /app
COPY --from=build /build/logic/logic_server /build/comet/comet_server /build/job/job_server /build/web_demo/web_demo_server /build/hermes_bridge/hermes_bridge /usr/local/bin/
COPY deploy /app/deploy
COPY conf /app/conf
COPY sql /app/sql
COPY web_demo/static /app/static
COPY LICENSE THIRD_PARTY_NOTICES.md /app/
COPY licenses /app/licenses
COPY third_party/pi/LICENSE /app/licenses/PI-MIT.txt
RUN mkdir -p /app/logs /app/data/attachments && chown -R spark:spark /app/logs /app/data
USER spark
ENTRYPOINT ["python3", "/app/deploy/entrypoint.py"]
CMD ["logic"]
