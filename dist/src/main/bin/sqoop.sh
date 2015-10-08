#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
function print_usage(){
  echo "Usage: sqoop.sh COMMAND"
  echo "       where COMMAND is one of:"
  echo "  server <start/stop/run>    Start/stop the server (or run it in the foreground)"
  echo "  client [script]            Start an interactive shell without a script"
  echo "                             or run a script with a batch shell"
  echo ""
}

function sqoop_server_classpath_set {

  HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME:-${HADOOP_HOME}/share/hadoop/common}
  HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME:-${HADOOP_HOME}/share/hadoop/hdfs}
  HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME:-${HADOOP_HOME}/share/hadoop/mapreduce}
  HADOOP_YARN_HOME=${HADOOP_YARN_HOME:-${HADOOP_HOME}/share/hadoop/yarn}

  if [[ ! (-d "${HADOOP_COMMON_HOME}" && -d "${HADOOP_HDFS_HOME}" && -d "${HADOOP_MAPRED_HOME}" && -d "${HADOOP_YARN_HOME}") ]]; then
    echo "Can't load the Hadoop related java lib, please check the setting for the following environment variables:"
    echo "    HADOOP_COMMON_HOME, HADOOP_HDFS_HOME, HADOOP_MAPRED_HOME, HADOOP_YARN_HOME"
    exit
  fi

  for f in $SQOOP_SERVER_LIB/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_COMMON_HOME/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_COMMON_HOME/lib/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_HDFS_HOME/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_HDFS_HOME/lib/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_MAPRED_HOME/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_MAPRED_HOME/lib/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_YARN_HOME/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HADOOP_YARN_HOME/lib/*.jar; do
    CLASSPATH="${CLASSPATH}:$f"
  done

  for f in $HIVE_HOME/lib/*.jar; do
    # exclude the jdbc for derby, to avoid the sealing violation exception
    if [[ ! $f =~ derby* && ! $f =~ jetty* ]]; then
      CLASSPATH="${CLASSPATH}:$f"
    fi
  done
}

function is_sqoop_server_running {
  if [[ -f "${sqoop_pidfile}" ]]; then
    kill -s 0 $(cat "$sqoop_pidfile") >/dev/null 2>&1
    return $?
  else
    return 1
  fi
}

function sqoop_extra_classpath_set {
  if [[ -n "${SQOOP_SERVER_EXTRA_LIB}" ]]; then
    for f in $SQOOP_SERVER_EXTRA_LIB/*.jar; do
      CLASSPATH="${CLASSPATH}:$f"
    done
  fi
}

if [ $# = 0 ]; then
  print_usage
  exit
fi

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

# Default configuration directory is relatively
DEFAULT_SQOOP_CONF_DIR=`dirname $0`/../conf
SQOOP_CONF_DIR=${SQOOP_CONF_DIR:-$DEFAULT_SQOOP_CONF_DIR}
echo "Setting conf dir: $SQOOP_CONF_DIR"

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`
SQOOP_IDENT_STRING=${SQOOP_IDENT_STRING:-$USER}
SQOOP_PID_DIR=${SQOOP_PID_DIR:-/tmp}
sqoop_pidfile="${SQOOP_PID_DIR}/sqoop-${SQOOP_IDENT_STRING}-jetty-server.pid"
JAVA_OPTS="$JAVA_OPTS -Dsqoop.config.dir=$SQOOP_CONF_DIR"

echo "Sqoop home directory: ${BASEDIR}"

SQOOP_CLIENT_LIB=${BASEDIR}/shell/lib
SQOOP_SERVER_LIB=${BASEDIR}/server/lib
SQOOP_TOOLS_LIB=${BASEDIR}/tools/lib

EXEC_JAVA='java'
if [ -n "${JAVA_HOME}" ] ; then
    EXEC_JAVA="${JAVA_HOME}/bin/java"
fi

# validation the java command
${EXEC_JAVA} -version 2>/dev/null
if [[ $? -gt 0 ]]; then
  echo "Can't find the path for java, please check the environment setting."
  exit
fi

sqoop_extra_classpath_set
COMMAND=$1
case $COMMAND in
  tool)
    if [ $# = 1 ]; then
      echo "Usage: sqoop.sh tool TOOL_NAME [TOOL_ARGS]"
      exit
    fi

    source ${BASEDIR}/bin/sqoop-sys.sh

    # Remove the "tool" keyword from the command line and pass the rest
    shift

    # Build class path with full path to each library,including tools ,server and hadoop related
    for f in $SQOOP_TOOLS_LIB/*.jar; do
      CLASSPATH="${CLASSPATH}:$f"
    done

    # Build class path with full path to each library, including hadoop related
    sqoop_server_classpath_set

    ${EXEC_JAVA} $JAVA_OPTS -classpath ${CLASSPATH} org.apache.sqoop.tools.ToolRunner $@
    ;;
  server)
    if [ $# = 1 ]; then
      echo "Usage: sqoop.sh server <start/stop>"
      exit
    fi

    source ${BASEDIR}/bin/sqoop-sys.sh

    case $2 in
      run)
        # For running in the foreground, we're not doing any checks if we're running or not and simply start the server)
        sqoop_server_classpath_set
        echo "Starting the Sqoop2 server..."
        exec ${EXEC_JAVA} $JAVA_OPTS -classpath ${CLASSPATH} org.apache.sqoop.server.SqoopJettyServer
        ;;
      start)
        # check if the sqoop server started already.
        is_sqoop_server_running
        if [[ $? -eq 0 ]]; then
          echo "The Sqoop server is already started."
          exit
        fi

        # Build class path with full path to each library, including hadoop related
        sqoop_server_classpath_set

        echo "Starting the Sqoop2 server..."
        ${EXEC_JAVA} $JAVA_OPTS -classpath ${CLASSPATH} org.apache.sqoop.server.SqoopJettyServer &

        echo $! > "${sqoop_pidfile}" 2>/dev/null
        if [[ $? -gt 0 ]]; then
          echo "ERROR:  Cannot write pid ${pidfile}."
        fi

        # wait 5 seconds, then check if the sqoop server started successfully.
        sleep 5
        is_sqoop_server_running
        if [[ $? -eq 0 ]]; then
          echo "Sqoop2 server started."
        fi
      ;;
      stop)
        # check if the sqoop server stopped already.
        is_sqoop_server_running
        if [[ $? -gt 0 ]]; then
          echo "No Sqoop server is running."
          exit
        fi

        pid=$(cat "$sqoop_pidfile")
        echo "Stopping the Sqoop2 server..."
        kill -9 "${pid}" >/dev/null 2>&1
        rm -f "${sqoop_pidfile}"
        echo "Sqoop2 server stopped."
      ;;
      *)
        echo "Unknown command, usage: sqoop.sh server <start/stop>"
        exit
      ;;
    esac
    ;;

  client)
    # Build class path with full path to each library
    for f in $SQOOP_CLIENT_LIB/*.jar; do
      CLASSPATH="${CLASSPATH}:$f"
    done

    ${EXEC_JAVA} $JAVA_OPTS -classpath ${CLASSPATH} org.apache.sqoop.shell.SqoopShell $2
    ;;

  *)
    echo "Command is not recognized."
    ;;
esac
