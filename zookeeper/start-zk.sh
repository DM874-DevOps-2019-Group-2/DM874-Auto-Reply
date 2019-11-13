#!/bin/bash

cat ${ZK_HOME}/conf/zoo.cfg
bash ${ZK_HOME}/bin/zkServer.sh start-foreground ${ZK_HOME}/conf/zoo.cfg