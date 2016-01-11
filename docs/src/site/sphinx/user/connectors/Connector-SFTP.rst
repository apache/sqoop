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


==============
SFTP Connector
==============

The SFTP connector supports moving data between a Secure File Transfer Protocol (SFTP) server and other supported Sqoop2 connectors.

Currently only the TO direction is supported to write records to an SFTP server. A FROM connector is pending (SQOOP-2218).

.. contents::
   :depth: 3

-----
Usage
-----

Before executing a Sqoop2 job with the SFTP connector, set **mapreduce.task.classpath.user.precedence** to true in the Hadoop cluster config, for example::

    <property>
      <name>mapreduce.task.classpath.user.precedence</name>
      <value>true</value>
    </property>

This is required since the SFTP connector uses the JSch library (http://www.jcraft.com/jsch/) to provide SFTP functionality. Unfortunately Hadoop currently ships with an earlier version of this library which causes an issue with some SFTP servers. Setting this property ensures that the current version of the library packaged with this connector will appear first in the classpath.

To use the SFTP Connector, create a link for the connector and a job that uses the link.

**Link Configuration**
++++++++++++++++++++++

Inputs associated with the link configuration include:

+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Input                       | Type    | Description                                                           | Example                    |
+=============================+=========+=======================================================================+============================+
| SFTP server hostname        | String  | Hostname for the SFTP server.                                         | sftp.example.com           |
|                             |         | *Required*.                                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| SFTP server port            | Integer | Port number for the SFTP server. Defaults to 22.                      | 2220                       |
|                             |         | *Optional*.                                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Username                    | String  | The username to provide when connecting to the SFTP server.           | sqoop                      |
|                             |         | *Required*.                                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Password                    | String  | The password to provide when connecting to the SFTP server.           | sqoop                      |
|                             |         | *Required*                                                            |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+

**Notes**
=========

1. The SFTP connector will attempt to connect to the SFTP server as part of the link validation process. If for some reason a connection can not be established, you'll see a corresponding error message.
2. Note that during connection, the SFTP connector explictly disables *StrictHostKeyChecking* to avoid "UnknownHostKey" errors.

**TO Job Configuration**
++++++++++++++++++++++++

Inputs associated with the Job configuration for the TO direction include:

+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Input                       | Type    | Description                                                             | Example                           |
+=============================+=========+=========================================================================+===================================+
| Output directory            | String  | The location on the SFTP server that the connector will write files to. | uploads                           |
|                             |         | *Required*                                                              |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+

**Notes**
=========

1. The *output directory* value needs to be an existing directory on the SFTP server.

------
Loader
------

During the *loading* phase, the connector will create uniquely named files in the *output directory* for each partition of data received from the **FROM** connector.