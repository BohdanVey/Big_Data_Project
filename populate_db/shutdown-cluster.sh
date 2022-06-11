#!/bin/bash

docker stop node1 node2 node3
docker rm node1 node2 node3
docker network rm cassandra-network1