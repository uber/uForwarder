#!/bin/bash
set -e

fievel_base_dir="fievel-common/src/main/java/com/uber/fievel/testing/base/"

function setup_fievel() {
  # generate FievelTestBase class to avoid duplication class check in fievel
  mkdir -p $fievel_base_dir
  fievel_base_file="$fievel_base_dir/FievelTestBase.java"
  printf "package com.uber.fievel.testing.base;\n" > "$fievel_base_file"
  printf "public abstract class FievelTestBase {}" >> "$fievel_base_file"
}

# setup fievel-common project
setup_fievel

# create idl folder
mkdir -p idl/src/main/proto/data/kafka/

ln -s ../../instrumentation/src instrumentation/src
ln -s ../../../../../../../../../idl/code.uber.internal/data/kafka/data-transfer idl/src/main/proto/data/kafka/data-transfer
ln -s ../../../../../../../../../idl/code.uber.internal/data/kafka/messaging-consumer idl/src/main/proto/data/kafka/messaging-consumer
ln -s ../../uforwarder-client/src uforwarder-client/src
ln -s ../../uforwarder-core/src uforwarder-core/src
ln -s ../../uforwarder/src uforwarder/src
ln -s ../../uforwarder-container/docker uforwarder-image/docker
ln -s ../../uforwarder-container/src uforwarder-container/src
