#!/bin/bash
docker container stop kafka-server
docker container rm kafka-server
docker container stop zookeeper-server
docker container rm zookeeper-server
docker network rm kafka-network