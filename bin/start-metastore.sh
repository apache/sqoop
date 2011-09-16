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
#
# This script starts a metastore instance.
# Usage: start-metastore.sh -p pidfilename -l logdir

prgm=$0
bin=`dirname $prgm`

while [ ! -z "$1" ]; do
  if [ "$1" == "-p" ]; then
    shift
    pidfilename=$1
    shift
  elif [ "$1" == "-l" ]; then
    shift
    logdir=$1
    shift
  else
    echo "Unknown argument $1"
    exit 1
  fi
done

# Verify our arguments exist.

if [ -z "${pidfilename}" ]; then
  echo "Missing argument: -p pidfilename"
  exit 1
fi

if [ -z "${logdir}" ]; then
  echo "Missing argument: -l logdir"
  exit 1
fi

if [ ! -d "${logdir}" ]; then
  echo "Warning: Log directory ${logdir} does not exist."
fi

function pid_file_alive() {
  local pidfile=$1 # IN
  local programname=$2 # IN
  local checkpid=`cat "$pidfile"`
  ps -fp $checkpid | grep $checkpid | grep "$programname" > /dev/null 2>&1
}

function fail_if_pid_exists() {
  local pidfile=$1 # IN
  local programname=$2 # IN
  if pid_file_alive "$pidfile" "$programname" ; then
    echo "Pid file $pidfile already exists; not starting metastore."
    exit 1
  fi
}

# Acquire the pidfile lock.
if [ -e "$pidfilename" ]; then
  # If the pid file exists, check to see if the process is alive.
  # We first write our own (bash script) pid into the pidfile.
  # Then we write the child pid over top; the bash script then terminates.
  # So we must be prepared to accept either case. 

  # We must check for bash first. Serialization matters.
  fail_if_pid_exists "$pidfilename" "bash"
  fail_if_pid_exists "$pidfilename" "sqoop"

  # We're good to go. Remove the existing pidfile.
  existingpid=`cat $pidfilename`
  [[ -e "$pidfilename" ]] && rm "$pidfilename" 
  [[ -e "$pidfilename.$existingpid" ]] && rm "$pidfilename.$existingpid"
fi

pid=$$
echo $pid > "$pidfilename.$pid"
if [ ! -e "$pidfilename.$pid" ]; then
  echo "Could not create pid file $pidfilename.$pid; not starting metastore."
  exit 1
fi

# Hardlink the "real" pidfile to our temporary one. This is atomic.
ln "$pidfilename.$pid" "$pidfilename"

# Verify that the real pidfile exists, and contains our current pid.
if [ ! -e "$pidfilename" ]; then
  echo "Could not create pid file $pidfilename; not starting metastore."
  exit 1
fi

val=`cat "$pidfilename"`
if [ "$val" != "$pid" ]; then
  # We lost the pid file race.
  echo "Metastore already started; not starting metastore."
  exit 1
fi

# Determine the log file name.
user=`id -un`
host=`hostname`

# Log file name we would like to use.
logfile="$logdir/sqoop-metastore-$user-$host.log"
touch $logfile >/dev/null 2>&1
if [ "$?" != "0" ]; then
  # Can't open for logging.
  echo "Warning: Cannot write to log directory. Disabling metastore log."
  logfile=/dev/null
fi

# Actually start the metastore.

if [ ! -z "$bin" ]; then
  bin="$bin/"
fi

nohup "$bin/sqoop" metastore > "$logfile" 2>&1 </dev/null &
ret=$?
realpid=$!

if [ "$ret" != "0" ]; then
  echo "Error starting metastore."
  rm "$pidfilename"
  rm "$pidfilename.$pid"
  exit $ret
fi

# Now replace the pid in the pidfile with the value in $realpid.
echo $realpid > "$pidfilename"

# The original pid file with the extension is no longer necessary.
rm "$pidfilename.$pid"


