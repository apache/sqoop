.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


=====================================
Sqoop 2 Development Environment Setup
=====================================

This document describes you how to setup development environment for Sqoop 2.

System Requirement
==================

Java
----

Sqoop has been developped and test only with JDK from `Oracle <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_ and we require at least version 7 (we're not supporting JDK 1.6 and older releases).

Maven
-----

Sqoop uses Maven 3 for building the project. Download `Maven <http://maven.apache.org/download.cgi>`_ and its Installation instructions given in `link <http://maven.apache.org/download.cgi#Maven_Documentation>`_.

Eclipse Setup
=============

Steps for downloading source code are given in :doc:`/dev/BuildingSqoop2`.

Sqoop 2 project has multiple modules where one module is depend on another module for e.g. sqoop 2 client module has sqoop 2 common module dependency. Follow below step for creating eclipse's project and classpath for each module.

::

  //Install all package into local maven repository
  mvn clean install -DskipTests

  //Adding M2_REPO variable to eclipse workspace
  mvn eclipse:configure-workspace -Declipse.workspace=<path-to-eclipse-workspace-dir-for-sqoop-2>

  //Eclipse project creation with optional parameters
  mvn eclipse:eclipse -DdownloadSources=true -DdownloadJavadocs=true

Alternatively, for manually adding M2_REPO classpath variable as maven repository path in eclipse-> window-> Java ->Classpath Variables ->Click "New" ->In new dialog box, input Name as M2_REPO and Path as $HOME/.m2/repository ->click Ok.

On successful execution of above maven commands, Then import the sqoop project modules into eclipse-> File -> Import ->General ->Existing Projects into Workspace-> Click Next-> Browse Sqoop 2 directory ($HOME/git/sqoop2) ->Click Ok ->Import dialog shows multiple projects (sqoop-client, sqoop-common, etc.) -> Select all modules -> click Finish.
