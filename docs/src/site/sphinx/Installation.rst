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


============
Installation
============

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

Hadoop libraries must be available on node where you are planning to run Sqoop server with proper configuration for major services - ``NameNode`` and either ``JobTracker`` or ``ResourceManager`` depending whether you are running Hadoop 1 or 2. There is no need to run any Hadoop service on the same node as Sqoop server, just the libraries and configuration files must be available.

Path to Hadoop libraries is stored in file ``catalina.properties`` inside directory ``server/conf``. You need to change property called ``common.loader`` to contain all directories with your Hadoop libraries. The default expected locations are ``/usr/lib/hadoop`` and ``/usr/lib/hadoop/lib/``. Please check out the comments in the file for further description how to configure different locations.

Lastly you might need to install JDBC drivers that are not bundled with Sqoop because of incompatible licenses. You can add any arbitrary Java jar file to Sqoop server by copying it into ``lib/`` directory. You can create this directory if it do not exists already.

Configuring PATH
----------------

All user and administrator facing shell commands are stored in ``bin/`` directory. It's recommended to add this directory to your ``$PATH`` for their easier execution, for example::

  PATH=$PATH:`pwd`/bin/

Further documentation pages will assume that you have the binaries on your ``$PATH``. You will need to call them specifying full path if you decide to skip this step.

Configuring Server
------------------

Before starting server you should revise configuration to match your specific environment. Server configuration files are stored in ``server/config`` directory of distributed artifact along side with other configuration files of Tomcat.

File ``sqoop_bootstrap.properties`` specifies which configuration provider should be used for loading configuration for rest of Sqoop server. Default value ``PropertiesConfigurationProvider`` should be sufficient.


Second configuration file ``sqoop.properties`` contains remaining configuration properties that can affect Sqoop server. File is very well documented, so check if all configuration properties fits your environment. Default or very little tweaking should be sufficient most common cases.

You can verify the Sqoop server configuration using `Verify Tool <Tools.html#verify>`__, for example::

  sqoop2-tool verify

Upon running the ``verify`` tool, you should see messages similar to the following::

  Verification was successful.
  Tool class org.apache.sqoop.tools.tool.VerifyTool has finished correctly

Consult `Verify Tool <Tools.html#upgrade>`__ documentation page in case of any failure.

Server Life Cycle
-----------------

After installation and configuration you can start Sqoop server with following command: ::

  sqoop2-server start

Similarly you can stop server using following command: ::

  sqoop2-server stop

By default Sqoop server daemons use ports 12000 and 12001. You can set ``SQOOP_HTTP_PORT`` and ``SQOOP_ADMIN_PORT`` in configuration file ``server/bin/setenv.sh`` to use different ports.

Client installation
===================

Client do not need extra installation and configuration steps. Just copy Sqoop distribution artifact on target machine and unzip it in desired location. You can start client with following command: ::

  sqoop2-shell

You can find more documentation to Sqoop client in `Command Line Client <CommandLineClient.html>`_ section.


