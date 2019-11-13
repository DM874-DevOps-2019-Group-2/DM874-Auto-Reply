FROM ubuntu:18.04

ARG kafka_version=2.3.0
ARG scala_version=2.12
ARG glibc_version=2.29-r0

LABEL org.label-schema.name="kafka" \
      org.label-schema.description="Apache Kafka" \
      org.label-schema.version="${scala_version}_${kafka_version}"

ENV KAFKA_VERSION=$kafka_version \
    SCALA_VERSION=$scala_version \
    KAFKA_HOME=/opt/kafka

ENV PATH=${PATH}:${KAFKA_HOME}/bin

RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get -y install default-jre-headless bash curl jq wget gnupg2

RUN apt-get update && apt-get install -y apt-transport-https gettext-base && \
    curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list && \
    apt-get update && \
    apt-get install -y kubectl

COPY download-kafka.sh start-kafka.sh /tmp/

RUN chmod a+x /tmp/*.sh && \
    mv /tmp/start-kafka.sh /usr/bin && \
    sync && /tmp/download-kafka.sh && \
    tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && \
    rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} && \
    rm -rf /tmp/*

RUN rm -rf /opt/kafka/config/server.properties

RUN mkdir -p /var/lib/kafka

#RUN apk add --no-cache bash curl jq docker \
# && chmod a+x /tmp/*.sh \
# && mv /tmp/start-kafka.sh /tmp/versions.sh /usr/bin \
# && sync && /tmp/download-kafka.sh \
# && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt \
# && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
# && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} \
# && rm /tmp/*

WORKDIR ${KAFKA_HOME}

# Use "exec" form so that it runs as PID 1 (useful for graceful shutdown)
CMD ["start-kafka.sh"]