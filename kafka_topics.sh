#!/bin/bash

kafka-topics --create --topic main_topic --partitions 3 --replication-factor 3 --if-not-exists --zookeeper 194.61.2.84:2181
kafka-topics --create --topic dead_letter --partitions 3 --replication-factor 3 --if-not-exists --zookeeper 194.61.2.84:2181
kafka-topics --describe --topic main_topic --zookeeper 194.61.2.84:22181

seq 1 | kafka-console-producer --broker-list 194.61.2.84:32770 --topic test && echo 'Produced 42 messages.'

kafka-console-consumer --bootstrap-server 194.61.2.84:32770 --topic main_topic --from-beginning --max-messages 42
kafka-console-consumer --bootstrap-server 194.61.2.84:32770 --topic dead_letter --from-beginning --max-messages 42