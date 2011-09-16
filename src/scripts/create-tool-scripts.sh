#!/bin/bash
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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Arguments to this script:
#   The output directory for the result scripts
#   The path to the script template file.
#   A file containing the list of tools, one per line.
# This script should be run in the base directory of the project.
# It will run 'bin/sqoop help' and determine the names of all subprograms,
# and then generate wrapper scripts for each of these.

set -e

bin=`dirname $0`
bin=`cd ${bin} && pwd`

outdir=$1
template=$2
toollistfile=$3

if [ -z "$outdir" ]; then
  echo "Output directory is required as the first argument."
  exit 1
fi

if [ -z "$template" ]; then
  echo "The script template filename is required as the second argument."
  exit 1
fi

if [ -z "$toollistfile" ]; then
  echo "The list of tools is not specified"
  exit 1
fi

if [ ! -d "$outdir" ]; then
  echo "Output directory does not exist!"
  exit 1
fi

if [ ! -f "$template" ]; then
  echo "Could not find template file: " $template
  exit 1
fi

if [ ! -f "$toollistfile" ]; then
  echo "Could not find tool list file: " $toollistfile
  exit 1
fi

# Query 'sqoop help' for the list of available commands. 
# The output of 'sqoop help' has been written to $toollistfile.
subprograms=`cat $toollistfile | awk ' BEGIN {use=0} /^$/ {use=0}  \
    { if (use) { print $1 } else { } } /^Available/ {use=1} '`

# For each of these, copy the template into place and sed the
# subcommand's name in.
for p in $subprograms; do
  target="${outdir}/sqoop-${p}"
  cp "${template}" "${target}"
  chmod +x "${target}"
  sed -i -e "s/SUBCOMMANDMARKER/$p/" "${target}"
done

