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


==================
FTP Connector
==================

The FTP connector supports moving data between an FTP server and other supported Sqoop2 connectors.

Currently only the TO direction is supported to write records to an FTP server. A FROM connector is pending (SQOOP-2127).

.. contents::
   :depth: 3

-----
Usage
-----

To use the FTP Connector, create a link for the connector and a job that uses the link.

**Link Configuration**
++++++++++++++++++++++

Inputs associated with the link configuration include:

+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Input                       | Type    | Description                                                           | Example                    |
+=============================+=========+=======================================================================+============================+
| FTP server hostname         | String  | Hostname for the FTP server.                                          | ftp.example.com            |
|                             |         | *Required*.                                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| FTP server port             | Integer | Port number for the FTP server. Defaults to 21.                       | 2100                       |
|                             |         | *Optional*.                                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Username                    | String  | The username to provide when connecting to the FTP server.            | sqoop                      |
|                             |         | *Required*.                                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Password                    | String  | The password to provide when connecting to the FTP server.            | sqoop                      |
|                             |         | *Required*                                                            |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+

**Notes**
=========

1. The FTP connector will attempt to connect to the FTP server as part of the link validation process. If for some reason a connection can not be established, you'll see a corresponding warning message.

**TO Job Configuration**
++++++++++++++++++++++++

Inputs associated with the Job configuration for the TO direction include:

+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Input                       | Type    | Description                                                             | Example                           |
+=============================+=========+=========================================================================+===================================+
| Output directory            | String  | The location on the FTP server that the connector will write files to.  | uploads                           |
|                             |         | *Required*                                                              |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+

**Notes**
=========

1. The *output directory* value needs to be an existing directory on the FTP server.

------
Loader
------

During the *loading* phase, the connector will create uniquely named files in the *output directory* for each partition of data received from the **FROM** connector.
