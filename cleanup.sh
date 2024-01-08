#!/bin/bash
set -e

fievel_base_dir="fievel-common/src/main/java/com/uber/fievel/testing/base/"

function cleanup_fievel() {
  rm "$fievel_base_dir/FievelTestBase.java"
}

cleanup_fievel

rm instrumentation/src
rm -r idl/src
rm uforwarder-client/src
rm uforwarder-core/src
rm uforwarder/src
rm uforwarder-image/docker
rm uforwarder-container/src
