#!/bin/bash

docker stop consumer
docker rm consumer
docker build -f Dockerfile_read -t  consumer .
docker run --network kafka-network --name consumer consumer

