#!/bin/bash


docker network create kafka-network

docker run --name cassandra-node1 --network kafka-network -d cassandra:latest

while ! docker exec -it cassandra-node1 cqlsh -e 'describe cluster' > /dev/null 2>&1; do
    sleep 1
done

echo "First node started"
# docker exec -it cassandra-node1 cqlsh -e "CREATE  KEYSPACE hw8_vey WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };" > ~/output
echo "Setup Cassandra Finished"

