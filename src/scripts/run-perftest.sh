#!/bin/bash
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

# run-perftest.sh
# USAGE:
# ./run-perftest.sh PerfTestClassName [arg arg arg...]
#
# This script will run one of the classes from src/perftest/ with the
# correct classpath set up.

bin=`dirname $0`
bin=`cd ${bin} && pwd`

# This is run in src/scripts/
SQOOP_HOME="${bin}/../../"

# Set up environment and classpath
source ${SQOOP_HOME}/bin/configure-sqoop "${bin}"

PERFTEST_CLASSES=${SQOOP_HOME}/build/perftest/classes

export HADOOP_CLASSPATH=${PERFTEST_CLASSES}:${SQOOP_JAR}:${HADOOP_CLASSPATH}
${HADOOP_COMMON_HOME}/bin/hadoop "$@"

