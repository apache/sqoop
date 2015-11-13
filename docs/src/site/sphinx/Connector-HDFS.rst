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
HDFS Connector
==============

.. contents::
   :depth: 3

-----
Usage
-----

To use the HDFS Connector, create a link for the connector and a job that uses the link.

**Link Configuration**
++++++++++++++++++++++

Inputs associated with the link configuration include:

+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Input                       | Type    | Description                                                           | Example                    |
+=============================+=========+=======================================================================+============================+
| URI                         | String  | The URI of the HDFS File System.                                      | hdfs://example.com:8020/   |
|                             |         | *Optional*. See note below.                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+
| Configuration directory     | String  | Path to the clusters configuration directory.                         | /etc/conf/hadoop           |
|                             |         | *Optional*.                                                           |                            |
+-----------------------------+---------+-----------------------------------------------------------------------+----------------------------+

**Notes**
=========

1. The specified URI will override the declared URI in your configuration.

**FROM Job Configuration**
++++++++++++++++++++++++++

Inputs associated with the Job configuration for the FROM direction include:

+-----------------------------+---------+-------------------------------------------------------------------------+------------------+
| Input                       | Type    | Description                                                             | Example          |
+=============================+=========+=========================================================================+==================+
| Input directory             | String  | The location in HDFS that the connector should look for files in.       | /tmp/sqoop2/hdfs |
|                             |         | *Required*. See note below.                                             |                  |
+-----------------------------+---------+-------------------------------------------------------------------------+------------------+
| Null value                  | String  | The value of NULL in the contents of each file extracted.               | \N               |
|                             |         | *Optional*. See note below.                                             |                  |
+-----------------------------+---------+-------------------------------------------------------------------------+------------------+
| Override null value         | Boolean | Tells the connector to replace the specified NULL value.                | true             |
|                             |         | *Optional*. See note below.                                             |                  |
+-----------------------------+---------+-------------------------------------------------------------------------+------------------+

**Notes**
=========

1. All files in *Input directory* will be extracted.
2. *Null value* and *override null value* should be used in conjunction. If *override null value* is not set to true, then *null value* will not be used when extracting data.

**TO Job Configuration**
++++++++++++++++++++++++

Inputs associated with the Job configuration for the TO direction include:

+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Input                       | Type    | Description                                                             | Example                           |
+=============================+=========+=========================================================================+===================================+
| Output directory            | String  | The location in HDFS that the connector will load files to.             | /tmp/sqoop2/hdfs                  |
|                             |         | *Optional*                                                              |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Output format               | Enum    | The format to output data to.                                           | CSV                               |
|                             |         | *Optional*. See note below.                                             |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Compression                 | Enum    | Compression class.                                                      | GZIP                              |
|                             |         | *Optional*. See note below.                                             |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Custom compression          | String  | Custom compression class.                                               | org.apache.sqoop.SqoopCompression |
|                             |         | *Optional* Comma separated list of columns.                             |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Null value                  | String  | The value of NULL in the contents of each file loaded.                  | \N                                |
|                             |         | *Optional*. See note below.                                             |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Override null value         | Boolean | Tells the connector to replace the specified NULL value.                | true                              |
|                             |         | *Optional*. See note below.                                             |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+
| Append mode                 | Boolean | Append to an existing output directory.                                 | true                              |
|                             |         | *Optional*.                                                             |                                   |
+-----------------------------+---------+-------------------------------------------------------------------------+-----------------------------------+

**Notes**
=========

1. *Output format* only supports CSV at the moment.
2. *Compression* supports all Hadoop compression classes.
3. *Null value* and *override null value* should be used in conjunction. If *override null value* is not set to true, then *null value* will not be used when loading data.

-----------
Partitioner
-----------

The HDFS Connector partitioner partitions based on total blocks in all files in the specified input directory.
Blocks will try to be placed in splits based on the *node* and *rack* they reside in.

---------
Extractor
---------

During the *extraction* phase, the FileSystem API is used to query files from HDFS. The HDFS cluster used is the one defined by:

1. The HDFS URI in the link configuration
2. The Hadoop configuration in the link configuration
3. The Hadoop configuration used by the execution framework

The format of the data must be CSV. The NULL value in the CSV can be chosen via *null value*. For example::

    1,\N
    2,null
    3,NULL

In the above example, if *null value* is set to \N, then only the first row's NULL value will be inferred.

------
Loader
------

During the *loading* phase, HDFS is written to via the FileSystem API. The number of files created is equal to the number of loads that run. The format of the data currently can only be CSV. The NULL value in the CSV can be chosen via *null value*. For example:

+--------------+-------+
| Id           | Value |
+==============+=======+
| 1            | NULL  |
+--------------+-------+
| 2            | value |
+--------------+-------+

If *null value* is set to \N, then here's how the data will look like in HDFS::

    1,\N
    2,value

----------
Destroyers
----------

The HDFS TO destroyer moves all created files to the proper output directory.
