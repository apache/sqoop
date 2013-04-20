#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function print() {
  if [ "${ECHO}" != "false" ]; then
    echo "$@"
  fi
}

ECHO="true"
if [ "${1}" == "-silent" ]; then
  ECHO="false"
fi

if [ "${SQOOP_HTTP_PORT}" = "" ]; then
  export SQOOP_HTTP_PORT=12000
  print "Setting SQOOP_HTTP_PORT:     ${SQOOP_HTTP_PORT}"
else
  print "Using   SQOOP_HTTP_PORT:     ${SQOOP_HTTP_PORT}"
fi

if [ "${SQOOP_ADMIN_PORT}" = "" ]; then
  export SQOOP_ADMIN_PORT=`expr $SQOOP_HTTP_PORT +  1`
  print "Setting SQOOP_ADMIN_PORT:     ${SQOOP_ADMIN_PORT}"
else
  print "Using   SQOOP_ADMIN_PORT:     ${SQOOP_ADMIN_PORT}"
fi
