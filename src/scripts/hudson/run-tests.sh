#!/bin/bash -x
#
# Copyright 2011 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Compiles the world and runs available unit tests.
# This script is intended for execution by users who want to thoroughly
# execute all tests, or automated testing agents such as Hudson.

# Environment:
# See test-config.sh

bin=`readlink -f $0`
bin=`dirname ${bin}`
bin=`cd ${bin} && pwd`
source ${bin}/test-config.sh

# Run compilation step.

${ANT} clean jar -Divy.home=$IVY_HOME -Dhadoop.dist=${COMPILE_HADOOP_DIST} \
    ${ANT_ARGUMENTS}
if [ "$?" != "0" ]; then
  echo "Error during compilation phase. Aborting!"
  exit 1
fi

testfailed=0

# Run basic unit tests.

${ANT} clean-cache test -Divy.home=$IVY_HOME -Dtest.junit.output.format=xml \
    -Dhadoop.dist=${TEST_HADOOP_DIST} ${ANT_ARGUMENTS}
if [ "$?" != "0" ]; then
  testfailed=1
fi

# Run thirdparty integration unit tests.

if [ "${THIRDPARTY_LIBS}" == "" ]; then
  echo "Warning: $$THIRDPARTY_LIBS not set."
fi

${ANT} test -Dthirdparty=true -Dsqoop.thirdparty.lib.dir=${THIRDPARTY_LIBS} \
    -Dtest.junit.output.format=xml -Divy.home=$IVY_HOME \
    -Dhadoop.dist=${TEST_HADOOP_DIST} ${ANT_ARGUMENTS}
if [ "$?" != "0" ]; then
  testfailed=1
fi

if [ "$testfailed" != "0" ]; then
  echo "Unit tests failed!"
  exit 1
fi

