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
Kite Connector
==============

.. contents::
   :depth: 3

-----
Usage
-----

To use the Kite Connector, create a link for the connector and a job that uses the link. For more information on Kite, checkout the kite documentation: http://kitesdk.org/docs/1.0.0/Kite-SDK-Guide.html.

**Link Configuration**
++++++++++++++++++++++

Inputs associated with the link configuration include:

+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Input                       | Type    | Description                                                           | Example                    |
+=============================+=========+=======================================================================+============================+
| authority                   | String  | The authority of the kite dataset.                                    | hdfs://example.com:8020/   |
|                             |         | *Optional*. See note below.                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+

**Notes**
=========

1. The authority is useful for specifying Hive metastore or HDFS URI.

**FROM Job Configuration**
++++++++++++++++++++++++++

Inputs associated with the Job configuration for the FROM direction include:

+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Input                       | Type    | Description                                                           | Example                    |
+=============================+=========+=======================================================================+============================+
| URI                         | String  | The Kite dataset URI to use.                                          | dataset:hdfs:/tmp/ns/ds    |
|                             |         | *Required*. See notes below.                                          |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+

**Notes**
=========

1. The URI and the authority from the link configuration will be merged to create a complete dataset URI internally. If the given dataset URI contains authority, the authority from the link configuration will be ignored.
2. Only *hdfs* and *hive* are supported currently.

**TO Job Configuration**
++++++++++++++++++++++++

Inputs associated with the Job configuration for the TO direction include:

+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Input                       | Type    | Description                                                           | Example                    |
+=============================+=========+=======================================================================+============================+
| URI                         | String  | The Kite dataset URI to use.                                          | dataset:hdfs:/tmp/ns/ds    |
|                             |         | *Required*. See note below.                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| File format                 | Enum    | The format of the data the kite dataset should write out.             | PARQUET                    |
|                             |         | *Optional*. See note below.                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+

**Notes**
=========

1. The URI and the authority from the link configuration will be merged to create a complete dataset URI internally. If the given dataset URI contains authority, the authority from the link configuration will be ignored.
2. Only *hdfs* and *hive* are supported currently.

-----------
Partitioner
-----------

The kite connector only creates one partition currently.

---------
Extractor
---------

During the *extraction* phase, Kite is used to query a dataset. Since there is only one dataset to query, only a single reader is created to read the dataset.

**NOTE**: The avro schema kite generates will be slightly different than the original schema. This is because avro identifiers have strict naming requirements.

------
Loader
------

During the *loading* phase, Kite is used to write several temporary datasets. The number of temporary datasets is equivalent to the number of *loaders* that are being used.

----------
Destroyers
----------

The Kite connector TO destroyer merges all the temporary datasets into a single dataset.