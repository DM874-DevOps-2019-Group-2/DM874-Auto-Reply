FROM openjdk:8u212-b04-jdk-stretch

MAINTAINER Valdemar Grange

ENV ZK_HOME /opt/zookeeper

RUN wget http://mirrors.dotsrc.org/apache/zookeeper/zookeeper-3.5.5/apache-zookeeper-3.5.5-bin.tar.gz

RUN tar -xzf apache-zookeeper-3.5.5-bin.tar.gz -C /opt

RUN ln -s /opt/apache-zookeeper-3.5.5-bin ${ZK_HOME}

RUN mkdir -p /data/zookeeper

ADD start-zk.sh /usr/bin/start-zk.sh
EXPOSE 2181 2888 3888

WORKDIR ${ZK_HOME}

RUN rm -rf /opt/zookeeper/conf/zoo.cfg

RUN chmod +x /usr/bin/start-zk.sh

CMD ["/usr/bin/start-zk.sh"]
