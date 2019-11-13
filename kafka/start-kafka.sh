#!/bin/bash
cat /opt/kafka/config/server.properties
rm -rf /var/lib/kafka/lost+found
bash /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties