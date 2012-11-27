.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF lANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


=======================================
Installation
=======================================

Sqoop ships as one binary package however it's compound from two separate parts - client and server. You need to install server on single node in your cluster. This node will then serve as an entry point for all connecting Sqoop clients. Server acts as a mapreduce client and therefore Hadoop must be installed and configured on machine hosting Sqoop server. Clients can be installed on any arbitrary number of machines. Client is not acting as a mapreduce client and thus you do not need to install Hadoop on nodes that will act only as a Sqoop client.

Server installation
===================

Copy Sqoop artifact on machine where you want to run Sqoop server. This machine must have installed and configured Hadoop. You don't need to run any Hadoop related services there, however the machine must be able to act as an Hadoop client. You should be able to list a HDFS for example: ::

  hadoop dfs -ls

Sqoop server supports multiple Hadoop versions. However as Hadoop major versions are not compatible with each other, Sqoop have multiple binary artefacts - one for each supported major version of Hadoop. You need to make sure that you're using appropriated binary artifact for your specific Hadoop version. To install Sqoop server decompress appropriate distribution artifact in location at your convenience and change your working directory to this folder. ::

  # Decompress Sqoop distribution tarball
  tar -xvf sqoop-<version>-bin-hadoop<hadoop-version>.tar.gz

  # Move decompressed content to any location
  mv sqoop-<version>-bin-hadoop<hadoop version>.tar.gz /usr/lib/sqoop

  # Change working directory
  cd /usr/lib/sqoop


Installing Dependencies
-----------------------

You need to install Hadoop libraries into Sqoop server war file. Sqoop provides convenience script ``addtowar.sh`` to do so. If you have installed Hadoop in usual location in ``/usr/lib`` and executable ``hadoop`` is in your path, you can use automatic Hadoop installation procedure: ::

  ./bin/addtowar.sh -hadoop-auto

In case that you have Hadoop installed in different location, you will need to manually specify Hadoop version and path to Hadoop libraries. You can use parameter ``-hadoop-version`` for specifying Hadoop major version, we're currently support versions 1.x and 2.x. Path to Hadoop libraries can be specified using ``-hadoop-path`` parameter. In case that your Hadoop libraries are in multiple different folders, you can specify all of them separated by ``:``.  Example of manual installation: ::

  ./bin/addtowar.sh -hadoop-version 2.0 -hadoop-path /usr/lib/hadoop-common:/usr/lib/hadoop-hdfs:/usr/lib/hadoop-yarn

Lastly you might need to install JDBC drivers that are not bundled with Sqoop because of incompatible licenses. You can add any arbitrary java jar file to Sqoop server using script ``addtowar.sh`` with ``-jars`` parameter. Similarly as in case of hadoop path you can enter multiple jars separated with ``:``. Example of installing MySQL JDBC driver to Sqoop server: ::

  ./bin/addtowar.sh -jars /path/to/jar/mysql-connector-java-*-bin.jar

Configuring Server
------------------

Before starting server you should revise configuration to match your specific environment. Server configuration files are stored in ``server/config`` directory of distributed artifact along side with other configuration files of Tomcat.

File ``sqoop_bootstrap.properties`` specifies which configuration provider should be used for loading configuration for rest of Sqoop server. Default value ``PropertiesConfigurationProvider`` should be sufficient.


Second configuration file ``sqoop.properties`` contains remaining configuration properties that can affect Sqoop server. File is very well documented, so check if all configuration properties fits your environment. Default or very little tweaking should be sufficient most common cases.

Server Life Cycle
-----------------

After installation and configuration you can start Sqoop server with following command: ::

  ./bin/sqoop.sh server start

Similarly you can stop server using following command: ::

  ./bin/sqoop.sh server stop


Client installation
===================

Client do not need extra installation and configuration steps. Just copy Sqoop distribution artifact on target machine and unzip it in desired location. You can start client with following command: ::

  bin/sqoop.sh client

You can find more documentation to Sqoop client in `Command Line Client <CommandLineClient.html>`_ section.


