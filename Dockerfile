# Copyright 2023 The SeamDB Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM --platform=$BUILDPLATFORM rust:1-bookworm AS build

ARG TARGETOS
ARG TARGETARCH

RUN apt update && apt install -y cmake protobuf-compiler clang wget unzip

ENV ETCD_VERSION=v3.5.17

RUN if [ "$TARGETOS" = "linux" ]; then \
        wget https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION/etcd-$ETCD_VERSION-$TARGETOS-$TARGETARCH.tar.gz && \
        mkdir /opt/etcd && \
        tar xvf etcd-$ETCD_VERSION-$TARGETOS-$TARGETARCH.tar.gz --strip-components 1 --directory=/opt/etcd; \
    elif [ "$TARGETOS" = "darwin" ]; then \
        wget https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION/etcd-$ETCD_VERSION-$TARGETOS-$TARGETARCH.zip && \
        unzip etcd-$ETCD_VERSION-$TARGETOS-$TARGETARCH.zip && \
        mv etcd-$ETCD_VERSION-$TARGETOS-$TARGETARCH /opt/etcd;  \
    else \
        echo "unsupported platform $BUILDPLATFORM"; \
    fi

RUN wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz && \
    mkdir /opt/kafka && \
    tar xvf kafka_2.13-3.9.0.tgz --strip-components 1 --directory=/opt/kafka

COPY . seamdb

RUN make -C seamdb release


# Final image here
FROM --platform=$BUILDPLATFORM debian:bookworm

RUN apt update && apt install -y openjdk-17-jdk wget lsof

COPY --from=build /opt/etcd /opt/etcd
COPY --from=build /opt/kafka /opt/kafka
COPY --from=build /seamdb/target/release/seamdbd /usr/bin/

ENV PATH=/opt/kafka/bin:/opt/etcd:$PATH

# https://kafka.apache.org/quickstart
RUN kafka-storage.sh format --standalone -t `kafka-storage.sh random-uuid` -c /opt/kafka/config/kraft/reconfig-server.properties

ENV RUST_BACKTRACE=full RUST_LOG=seamdb=trace

RUN rm -rf entrypoint.sh && \
    echo "kafka-server-start.sh /opt/kafka/config/kraft/reconfig-server.properties >> /var/log/kafka.log 2>&1 &" >> entrypoint.sh && \
    echo "" >> entrypoint.sh && \
    echo "etcd >> /var/log/etcd.log 2>&1 &" >> entrypoint.sh && \
    echo "" >> entrypoint.sh && \
    echo "until lsof -i :2379 > /dev/null" >> entrypoint.sh && \
    echo "do" >> entrypoint.sh && \
    echo "  echo 'waiting etcd up'" >> entrypoint.sh && \
    echo "  sleep 1" >> entrypoint.sh && \
    echo "done" >> entrypoint.sh && \
    echo "echo 'etcd is ready'" >> entrypoint.sh && \
    echo "" >> entrypoint.sh && \
    echo "until lsof -i :9092 > /dev/null" >> entrypoint.sh && \
    echo "do" >> entrypoint.sh && \
    echo "  echo 'waiting kafka up'" >> entrypoint.sh && \
    echo "  sleep 1" >> entrypoint.sh && \
    echo "done" >> entrypoint.sh && \
    echo "echo 'kafka is ready'" >> entrypoint.sh && \
    echo "" >> entrypoint.sh && \
    echo "seamdbd --cluster.uri etcd://127.0.0.1:2379/seamdb1 --log.uri kafka://127.0.0.1:9092" >> entrypoint.sh && \
    chmod u+x entrypoint.sh

EXPOSE 5432

CMD ./entrypoint.sh
