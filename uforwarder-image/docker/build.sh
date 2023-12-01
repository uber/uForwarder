#!/bin/bash
set -e

# clean up binary when exit
clean_up () {
    rm -f ./bin_main.jar
}
trap clean_up EXIT

# copy executable from binary output dir to local folder
cp ../../../../buck-out/gen/data/kafka/uforwarder/bin_main.jar .

# build docker image uforwarder:latest
docker build -t uforwarder:0.1 .
