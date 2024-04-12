#!/bin/bash
set -e

# clean up binary when exit
clean_up () {
    rm -f ./bin_main.jar
}
trap clean_up EXIT

# copy executable from binary output dir to local folder
cp ../../uforwarder/build/libs/uforwarder-0.1-SNAPSHOT.jar ./bin_main.jar

# build docker image uforwarder:latest
docker build -t uforwarder:0.1 .
