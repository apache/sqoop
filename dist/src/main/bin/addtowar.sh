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

# Creating temporary directory
function prepare() {
  tmpDir=/tmp/sqoop-war-packing-$$
  rm -rf ${tmpDir}
  mkdir ${tmpDir}
  tmpWarDir=${tmpDir}/sqoop-war
  mkdir ${tmpWarDir}
  checkExec "Creating staging directory ${tmpDir}"
}

# Cleans up temporary directory
function cleanUp() {
  if [ ! "${tmpDir}" = "" ]; then
    rm -rf ${tmpDir}
    checkExec "Deleting staging directory ${tmpDir}"
  fi
}

# Check execution of command
function checkExec() {
  if [ $? -ne 0 ]
  then
    echo
    echo "Failed: $1"
    echo
    cleanUp
    exit -1;
  fi
}

# Check that a file/path exists
function checkFileExists() {
  if [ ! -e ${1} ]; then
    echo
    echo "File/Dir does no exist: ${1}"
    echo
    cleanUp
    exit -1
  fi
}

# Check that a file/path does not exist
function checkFileDoesNotExist() {
  if [ -e ${1} ]; then
    echo
    echo "File/Dir already exists: ${1}"
    echo
    cleanUp
    exit -1
  fi
}

# Finds a file under a directory any depth, file returns in variable RET
function findFile() {
   RET=`find -H ${1} -name ${2} | grep -e "[0-9.a${hadoopJarsSuffix}].jar"`
   RET=`echo ${RET} | sed "s/ .*//"`
   if [ "${RET}" = "" ]; then
     echo
     echo "File '${2}' not found in '${1}'"
     echo
     cleanUp
     exit -1;
   fi
}

function checkOption() {
  if [ "$2" = "" ]; then
    echo
    echo "Missing option: ${1}"
    echo
    printUsage
    exit -1
  fi
}

# Get the list of hadoop jars that will be injected based on the hadoop version
# TODO(jarcec): Add configuration specific to Hadoop 1.x
function getHadoopJars() {
  version=$1
  if [[ "${version}" =~ 1.* ]]; then
    #List is separated by ":"
    hadoopJars="hadoop-core*.jar:jackson-core-asl-*.jar:jackson-mapper-asl-*.jar:commons-configuration-*.jar:commons-logging-*.jar:slf4j-api-*.jar:slf4j-log4j*.jar"
  elif [[ "${version}" =~ 2.* ]]; then
    suffix="-[0-9.]*"
    # List is separated by ":"
    hadoopJars="hadoop-mapreduce-client-core${suffix}.jar:hadoop-mapreduce-client-common${suffix}.jar:hadoop-mapreduce-client-jobclient${suffix}.jar:hadoop-mapreduce-client-app${suffix}.jar:hadoop-yarn-common${suffix}.jar:hadoop-yarn-api${suffix}.jar:hadoop-hdfs${suffix}.jar:hadoop-common${suffix}.jar:hadoop-auth${suffix}.jar:guava*.jar:protobuf-*.jar:jackson-core-asl-*.jar:jackson-mapper-asl-*.jar:commons-configuration-*.jar:commons-cli-*.jar:commons-logging-*.jar:slf4j-api-*.jar:slf4j-log4j*.jar:avro-*.jar"
  else
    echo
    echo "Exiting: Unsupported Hadoop version '${hadoopVer}', supported versions: 1.x, 2.x"
    echo
    cleanUp
    exit -1;
  fi
}

function printUsage() {
  echo " Usage  : addtowar.sh <OPTIONS>"
  echo " Options: -hadoop HADOOP_VERSION HADOOP_PATH"
  echo "          [-jars JARS_PATH] (multiple JAR path separated by ':')"
  echo "          [-war SQOOP_WAR]"
  echo
}

# We need at least some arguments
if [ $# -eq 0 ]; then
  echo
  echo "Missing options"
  echo
  printUsage
  exit -1
fi

# Variables that will be populated by our command line arguments
addHadoop=""
addJars=""
hadoopVersion=""
hadoopHome=""
jarsPath=""
warPath="`dirname $0`/../server/webapps/sqoop.war"

# Parse command line arguments
while [ $# -gt 0 ]
do
  if [ "$1" = "-hadoop" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, Hadoop version"
      echo
      printUsage
      exit -1
    fi
    hadoopVersion=$1
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, Hadoop path"
      echo
      printUsage
      exit -1
    fi
    hadoopHome=$1
    addHadoop=true
  elif [ "$1" = "-jars" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, JARs path"
      echo
      printUsage
      exit -1
    fi
    jarsPath=$1
    addJars=true
  elif [ "$1" = "-war" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, Input Sqoop WAR path"
      echo
      printUsage
      exit -1
    fi
    warPath=$1
  fi

  shift
done

# Check that we have something to do
if [ "${addHadoop}${addJars}" == "" ]; then
  echo
  echo "Nothing to do"
  echo
  printUsage
  exit -1
fi

prepare

checkOption "-war" ${warPath}
checkFileExists ${warPath}

if [ "${addHadoop}" = "true" ]; then
  checkFileExists ${hadoopHome}
  getHadoopJars ${hadoopVersion}
fi

if [ "${addJars}" = "true" ]; then
  for jarPath in ${jarsPath//:/$'\n'}
  do
    checkFileExists ${jarPath}
  done
fi

# Unpacking original war
unzip ${warPath} -d ${tmpWarDir} > /dev/null
checkExec "Unzipping Sqoop WAR"

components=""

# Adding hadoop binaries to WAR file
if [ "${addHadoop}" = "true" ]; then
  components="Hadoop JARs";
  found=`ls ${tmpWarDir}/WEB-INF/lib/hadoop*core*jar 2> /dev/null | wc -l`
  checkExec "Looking for Hadoop JARs in WAR file"
  if [ ! $found = 0 ]; then
    echo
    echo "Specified Sqoop WAR '${warPath}' already contains Hadoop JAR files"
    echo
    cleanUp
    exit -1
  fi
  ## adding hadoop
  echo "Injecting following Hadoop JARs"
  echo
  for jar in ${hadoopJars//:/$'\n'}
  do
    findFile ${hadoopHome} ${jar}
    jar=${RET}
    echo ${jar}
    cp ${jar} ${tmpWarDir}/WEB-INF/lib/
    checkExec "Copying jar ${jar} to staging"
  done
fi

# Adding new jars to WAR file
if [ "${addJars}" = "true" ]; then
  if [ ! "${components}" = "" ];then
    components="${components}, "
  fi
  components="${components}JARs"

  for jarPath in ${jarsPath//:/$'\n'}
  do
    found=`ls ${tmpWarDir}/WEB-INF/lib/${jarPath} 2> /dev/null | wc -l`
    checkExec "Looking for JAR ${jarPath} in WAR path"
    if [ ! $found = 0 ]; then
      echo
      echo "Specified Sqoop WAR '${inputWar}' already contains JAR ${jarPath}"
      echo
      cleanUp
      exit -1
    fi
    cp ${jarPath} ${tmpWarDir}/WEB-INF/lib/
    checkExec "Copying jar ${jarPath} to staging"
  done
fi

# Creating new Sqoop WAR
currentDir=`pwd`
cd ${tmpWarDir}
zip -r sqoop.war * > /dev/null
checkExec "Creating new Sqoop WAR"
cd ${currentDir}

# Save original WAR file as a backup in case that something went wrong
backupPath="${warPath}_`date +%Y-%m-%d_%H:%M:%S.%N`"
echo
echo "Backing up original WAR file to $backupPath"
mv $warPath $backupPath
checkExec "Backing up original WAR file to $backupPath"

# Move our jar to new position
mv ${tmpWarDir}/sqoop.war ${warPath}
checkExec "Moving generated WAR file to original location"

echo
echo "New Sqoop WAR file with added '${components}' at ${warPath}"
echo
cleanUp
exit 0
