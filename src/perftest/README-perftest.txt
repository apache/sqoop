
  Copyright 2011 The Apache Software Foundation
 
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.


= Performance and Stress Tests

The files in this directory represent performance or stress tests of aspects
of Sqoop. These are intended to be run by a developer as a part of a lengthy
QA process but are not the "primary" tests which are expected to be run for
every build.


== Compiling

To compile the tests in this directory, run 'ant jar compile-perf-test' in the
project root.

== Running

After compiling the performance tests, you can run them with:

$ src/scripts/run-perftest.sh <PerfTestClassName> [<args...>]

e.g.:

$ src/scripts/run-perftest.sh LobFileStressTest

