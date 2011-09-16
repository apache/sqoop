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
# Configures environment variables for run-tests.sh and run-code-quality.sh.

# Environment:
#
# ANT - path to 'ant' executable
# COBERTURA_HOME - The path to Cobertura
# COMPILE_HADOOP_DIST - Hadoop distribution to use for compilation phases
#    Values are:
#      "apache" for Apache trunk
#      "apache21" for Apache branch 0.21
#      "cloudera" for CDH 3 beta 2
# FINDBUGS_HOME - The path to findbugs
# IVY_HOME - path where ivy should cache objects.
# TEST_HADOOP_DIST - Hadoop distribution to use for testing phases
# THIRDPARTY_LIBS - Path to the thirdparty library directory.
# WORKSPACE - The workspace for running the build. Defaults to the project
#     root directory.

bin=`readlink -f $0`
bin=`dirname ${bin}`
bin=`cd ${bin} && pwd`
export projroot="${bin}/../../../"

export ANT=${ANT:-/bin/ant}
export COMPILE_HADOOP_DIST=${COMPILE_HADOOP_DIST:-apache}
export TEST_HADOOP_DIST=${TEST_HADOOP_DIST:-apache}

export WORKSPACE=${WORKSPACE:-$projroot}
export IVY_HOME=${IVY_HOME:-$WORKSPACE/.ivy2}

export HBASE_HOME=${HBASE_HOME:-/usr/lib/hbase}
export ZOOKEEPER_HOME=${ZOOKEEPER_HOME:-/usr/lib/zookeeper}

if [ -z "${ANT_ARGUMENTS}" ]; then
  export ANT_ARGUMENTS=""
fi

