#!/bin/bash
/var/lib/kafka-0.8.2/bin/kafka-console-consumer.sh --topic twitter --from-beginning --zookeeper 10.240.163.8:2181 | { I=0; while read; do printf "$((++I))\r"; done; echo ""; }

