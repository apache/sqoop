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

Sqoop ships as one binary package that incorporates two separate parts - client and server.

* **Server** You need to install server on single node in your cluster. This node will then serve as an entry point for all Sqoop clients.
* **Client** Clients can be installed on any number of machines.

Server installation
===================

Copy the Sqoop artifact to the machine where you want to run Sqoop server. The Sqoop server acts as a Hadoop client, therefore Hadoop libraries (Yarn, Mapreduce, and HDFS jar files) and configuration files (``core-site.xml``, ``mapreduce-site.xml``, ...) must be available on this node. You do not need to run any Hadoop related services - running the server on a "gateway" node is perfectly fine.

You should be able to list a HDFS for example:

.. code-block:: bash

  hadoop dfs -ls

Sqoop currently supports Hadoop version 2.6.0 or later. To install the Sqoop server, decompress the tarball (in a location of your choosing) and set the newly created forder as your working directory.

.. code-block:: bash

  # Decompress Sqoop distribution tarball
  tar -xvf sqoop-<version>-bin-hadoop<hadoop-version>.tar.gz

  # Move decompressed content to any location
  mv sqoop-<version>-bin-hadoop<hadoop version>.tar.gz /usr/lib/sqoop

  # Change working directory
  cd /usr/lib/sqoop


Hadoop dependencies
-------------------

Sqoop server needs following environmental variables pointing at Hadoop libraries - ``$HADOOP_COMMON_HOME``, ``$HADOOP_HDFS_HOME``, ``$HADOOP_MAPRED_HOME`` and ``$HADOOP_YARN_HOME``. You have to make sure that those variables are defined and pointing to a valid Hadoop installation. Sqoop server will not start if Hadoop libraries can't be found.

The Sqoop server uses environment variables to find Hadoop libraries. If the environment variable ``$HADOOP_HOME`` is set, Sqoop will look for jars in the following locations: ``$HADOOP_HOME/share/hadoop/common``, ``$HADOOP_HOME/share/hadoop/hdfs``, ``$HADOOP_HOME/share/hadoop/mapreduce`` and ``$HADOOP_HOME/share/hadoop/yarn``. You can specify where the Sqoop server should look for the common, hdfs, mapreduce, and yarn jars indepently with the ``$HADOOP_COMMON_HOME``, ``$HADOOP_HDFS_HOME``, ``$HADOOP_MAPRED_HOME`` and ``$HADOOP_YARN_HOME`` environment variables.


.. code-block:: bash

  # Export HADOOP_HOME variable
  export HADOOP_HOME=/...

  # Or alternatively HADOOP_*_HOME variables
  export HADOOP_COMMON_HOME=/...
  export HADOOP_HDFS_HOME=/...
  export HADOOP_MAPRED_HOME=/...
  export HADOOP_YARN_HOME=/...

.. note::

  If the environment ``$HADOOP_HOME`` is set, Sqoop will usee the following locations: ``$HADOOP_HOME/share/hadoop/common``, ``$HADOOP_HOME/share/hadoop/hdfs``, ``$HADOOP_HOME/share/hadoop/mapreduce`` and ``$HADOOP_HOME/share/hadoop/yarn``.

Hadoop configuration
--------------------

Sqoop server will need to impersonate users to access HDFS and other resources in or outside of the cluster as the user who started given job rather then user who is running the server. You need to configure Hadoop to explicitly allow this impersonation via so called proxyuser system. You need to create two properties in  ``core-site.xml`` file - ``hadoop.proxyuser.$SERVER_USER.hosts`` and ``hadoop.proxyuser.$SERVER_USER.groups`` where ``$SERVER_USER`` is the user who will be running Sqoop 2 server. In most scenarios configuring ``*`` is sufficient. Please refer to Hadoop documentation for details how to use those properties.

Example fragment that needs to be present in ``core-site.xml`` file for case when server is running under ``sqoop2`` user:

.. code-block:: xml

  <property>
    <name>hadoop.proxyuser.sqoop2.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.sqoop2.groups</name>
    <value>*</value>
  </property>

If you're running Sqoop 2 server under a so called system user (user with ID less then ``min.user.id`` - 1000 by default), then YARN will by default refuse to run Sqoop 2 jobs. You will need to add the user name who is running Sqoop 2 server (most likely user ``sqoop2``) to a ``allowed.system.users`` property of ``container-executor.cfg``. Please refer to YARN documentation for further details.

Example fragment that needs to be present in ``container-executor.cfg`` file for case when server is running under ``sqoop2`` user:

.. code-block:: xml

  allowed.system.users=sqoop2

Third party jars
----------------

To propagate any third party jars to Sqoop server classpath, create a directory anywhere on the file system and export it's location in ``SQOOP_SERVER_EXTRA_LIB`` variable.

.. code-block:: bash

  # Create directory for extra jars
  mkdir -p /var/lib/sqoop2/

  # Copy all your JDBC drivers to this directory
  cp mysql-jdbc*.jar /var/lib/sqoop2/
  cp postgresql-jdbc*.jar /var/lib/sqoop2/

  # And finally export this directory to SQOOP_SERVER_EXTRA_LIB
  export SQOOP_SERVER_EXTRA_LIB=/var/lib/sqoop2/

.. note::

  Sqoop doesn't ship with any JDBC drivers due to incompatible licenses. You will need to use this mechanism to install all JDBC drivers that are needed.

Configuring ``PATH``
--------------------

All user and administrator facing shell commands are stored in ``bin/`` directory. It's recommended to add this directory to your ``$PATH`` for easier execution, for example:

.. code-block:: bash

  PATH=$PATH:`pwd`/bin/

The remainder of the Sqoop 2 documentation assumes that the shell commands are in your ``$PATH``.

Configuring Server
------------------

Server configuration files are stored in ``conf`` directory. File ``sqoop_bootstrap.properties`` specifies which configuration provider should be used for loading configuration for rest of Sqoop server. Default value ``PropertiesConfigurationProvider`` should be sufficient.

Second configuration file called ``sqoop.properties`` contains remaining configuration properties that can affect Sqoop server. The configuration file is very well documented, so check if all configuration properties fits your environment. Default or very little tweaking should be sufficient in most common cases.

Repository Initialization
-------------------------

The metadata repository needs to be initialized before starting Sqoop 2 server for the first time. Use :ref:`tool-upgrade` to initialize the repository:

.. code-block:: bash

  sqoop2-tool upgrade

You can verify if everything have been configured correctly using :ref:`tool-verify`:

.. code-block:: bash

  sqoop2-tool verify
  ...
  Verification was successful.
  Tool class org.apache.sqoop.tools.tool.VerifyTool has finished correctly

Server Life Cycle
-----------------

After installation and configuration you can start Sqoop server with following command:

.. code-block:: bash

  sqoop2-server start

You can stop the server using the following command:

.. code-block:: bash

  sqoop2-server stop

By default Sqoop server daemon use port ``12000``. You can set ``org.apache.sqoop.jetty.port`` in configuration file ``conf/sqoop.properties`` to use different port.

Client installation
===================

Just copy Sqoop distribution artifact on target machine and unzip it in desired location. You can start client with following command:

.. code-block:: bash

  sqoop2-shell

You can find more documentation for Sqoop shell in :doc:`/user/CommandLineClient`.

.. note::

  Client is not acting as a Hadoop client and thus you do not need to be installed on node with Hadoop libraries and configuration files.