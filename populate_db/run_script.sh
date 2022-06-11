#!/bin/bash

docker stop consumer
docker rm consumer
docker build -t consumer .
docker run --network kafka-network --name consumer consumer

