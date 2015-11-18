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


================================
Building Sqoop2 from source code
================================

This guide will show you how to build Sqoop2 from source code. Sqoop is using `maven <http://maven.apache.org/>`_ as build system. You you will need to use at least version 3.0 as older versions will not work correctly. All other dependencies will be downloaded by maven automatically. With exception of special JDBC drivers that are needed only for advanced integration tests.

Downloading source code
-----------------------

Sqoop project is using git as a revision control system hosted at Apache Software Foundation. You can clone entire repository using following command:

::

  git clone https://git-wip-us.apache.org/repos/asf/sqoop.git sqoop2

Sqoop2 is currently developed in special branch ``sqoop2`` that you need to check out after clone:

::

  cd sqoop2
  git checkout sqoop2

Building project
----------------

You can use usual maven targets like ``compile`` or ``package`` to build the project. Sqoop supports one major Hadoop revision at the moment - 2.x. As compiled code for one Hadoop major version can't be used on another, you must compile Sqoop against appropriate Hadoop version.

::

  mvn compile

Maven target ``package`` can be used to create Sqoop packages similar to the ones that are officially available for download. Sqoop will build only source tarball by default. You need to specify ``-Pbinary`` to build binary distribution.

::

  mvn package -Pbinary

Running tests
-------------

Sqoop supports two different sets of tests. First smaller and much faster set is called **unit tests** and will be executed on maven target ``test``. Second larger set of **integration tests** will be executed on maven target ``integration-test``. Please note that integration tests might require manual steps for installing various JDBC drivers into your local maven cache.

Example for running unit tests:

::

  mvn test

Example for running integration tests:

::

  mvn integration-test

For the **unit tests**, there are two helpful profiles: **fast** and **slow**. The **fast** unit tests do not start or use any services. The **slow** unit tests, may start services or use an external service (ie. MySQL).

::

  mvn test -Pfast,hadoop200
  mvn test -Pslow,hadoop200