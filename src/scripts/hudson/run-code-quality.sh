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
# Compiles the world and runs code quality/test coverage checks.
# This script is intended for execution by users who want to thoroughly
# execute all tests, or automated testing agents such as Hudson.

# Environment:
# See test-config.sh

bin=`readlink -f $0`
bin=`dirname ${bin}`
bin=`cd ${bin} && pwd`
source ${bin}/test-config.sh

if [ -z "${FINDBUGS_HOME}" ]; then
  echo "Error: $$FINDBUGS_HOME is not set."
  exit 1
fi

if [ -z "${COBERTURA_HOME}" ]; then
  echo "Error: $$COBERTURA_HOME is not set."
  exit 1
fi

# Run main compilation step.

${ANT} clean jar-all findbugs javadoc cobertura checkstyle \
    -Divy.home=$IVY_HOME -Dhadoop.dist=${COMPILE_HADOOP_DIST} \
    -Dcobertura.home=${COBERTURA_HOME} -Dcobertura.format=xml \
    -Dfindbugs.home=${FINDBUGS_HOME} \
    -Dtest.junit.output.format=xml ${ANT_ARGUMENTS}

if [ "$?" != "0" ]; then
  echo "Error during compilation phase. Aborting!"
  exit 1
fi

# Run second cobertura step on thirdparty tests.
${ANT} cobertura \
    -Divy.home=$IVY_HOME -Dtest.junit.output.format=xml \
    -Dhadoop.dist=${COMPILE_HADOOP_DIST} \
    -Dcobertura.home=${COBERTURA_HOME} -Dcobertura.format=xml \
    -Dsqoop.thirdparty.lib.dir=${THIRDPARTY_LIBS} \
    -Dtestcase=ThirdPartyTests ${ANT_ARGUMENTS}

if [ "$?" != "0" ]; then
  echo "Unit tests failed!"
  exit 1
fi

