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
Sqoop 2 Connectors
==================

This document describes how to use the built-in connectors. This includes a detailed description of how connectors partition, format their output, extract data, and load data.

.. contents::
   :depth: 3

++++++++++++++++++++++
Generic JDBC Connector
++++++++++++++++++++++

The Generic JDBC Connector can connect to any data source that adheres to the **JDBC 4** specification.

-----
Usage
-----

To use the Generic JDBC Connector, create a link for the connector and a job that uses the link.

**Link Configuration**
++++++++++++++++++++++

Inputs associated with the link configuration include:

+-----------------------------+---------+-----------------------------------------------------------------------+------------------------------------------+
| Input                       | Type    | Description                                                           | Example                                  |
+=============================+=========+=======================================================================+==========================================+
| JDBC Driver Class           | String  | The full class name of the JDBC driver.                               | com.mysql.jdbc.Driver                    |
|                             |         | *Required* and accessible by the Sqoop server.                        |                                          |
+-----------------------------+---------+-----------------------------------------------------------------------+------------------------------------------+
| JDBC Connection String      | String  | The JDBC connection string to use when connecting to the data source. | jdbc:mysql://localhost/test              |
|                             |         | *Required*. Connectivity upon creation is optional.                   |                                          |
+-----------------------------+---------+-----------------------------------------------------------------------+------------------------------------------+
| Username                    | String  | The username to provide when connecting to the data source.           | sqoop                                    |
|                             |         | *Optional*. Connectivity upon creation is optional.                   |                                          |
+-----------------------------+---------+-----------------------------------------------------------------------+------------------------------------------+
| Password                    | String  | The password to provide when connecting to the data source.           | sqoop                                    |
|                             |         | *Optional*. Connectivity upon creation is optional.                   |                                          |
+-----------------------------+---------+-----------------------------------------------------------------------+------------------------------------------+
| JDBC Connection Properties  | Map     | A map of JDBC connection properties to pass to the JDBC driver        | profileSQL=true&useFastDateParsing=false |
|                             |         | *Optional*.                                                           |                                          |
+-----------------------------+---------+-----------------------------------------------------------------------+------------------------------------------+

**FROM Job Configuration**
++++++++++++++++++++++++++

Inputs associated with the Job configuration for the FROM direction include:

+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+
| Input                       | Type    | Description                                                             | Example                                     |
+=============================+=========+=========================================================================+=============================================+
| Schema name                 | String  | The schema name the table is part of.                                   | sqoop                                       |
|                             |         | *Optional*                                                              |                                             |
+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+
| Table name                  | String  | The table name to import data from.                                     | test                                        |
|                             |         | *Optional*. See note below.                                             |                                             |
+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+
| Table SQL statement         | String  | The SQL statement used to perform a **free form query**.                | ``SELECT COUNT(*) FROM test ${CONDITIONS}`` |
|                             |         | *Optional*. See notes below.                                            |                                             |
+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+
| Table column names          | String  | Columns to extract from the JDBC data source.                           | col1,col2                                   |
|                             |         | *Optional* Comma separated list of columns.                             |                                             |
+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+
| Partition column name       | Map     | The column name used to partition the data transfer process.            | col1                                        |
|                             |         | *Optional*.  Defaults to table's first column of primary key.           |                                             |
+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+
| Null value allowed for      | Boolean | True or false depending on whether NULL values are allowed in data      | true                                        |
| the partition column        |         | of the Partition column. *Optional*.                                    |                                             |
+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+
| Boundary query              | String  | The query used to define an upper and lower boundary when partitioning. |                                             |
|                             |         | *Optional*.                                                             |                                             |
+-----------------------------+---------+-------------------------------------------------------------------------+---------------------------------------------+

**Notes**
=========

1. *Table name* and *Table SQL statement* are mutually exclusive. If *Table name* is provided, the *Table SQL statement* should not be provided. If *Table SQL statement* is provided then *Table name* should not be provided.
2. *Table column names* should be provided only if *Table name* is provided.
3. If there are columns with similar names, column aliases are required. For example: ``SELECT table1.id as "i", table2.id as "j" FROM table1 INNER JOIN table2 ON table1.id = table2.id``.

**TO Job Configuration**
++++++++++++++++++++++++

Inputs associated with the Job configuration for the TO direction include:

+-----------------------------+---------+-------------------------------------------------------------------------+-------------------------------------------------+
| Input                       | Type    | Description                                                             | Example                                         |
+=============================+=========+=========================================================================+=================================================+
| Schema name                 | String  | The schema name the table is part of.                                   | sqoop                                           |
|                             |         | *Optional*                                                              |                                                 |
+-----------------------------+---------+-------------------------------------------------------------------------+-------------------------------------------------+
| Table name                  | String  | The table name to import data from.                                     | test                                            |
|                             |         | *Optional*. See note below.                                             |                                                 |
+-----------------------------+---------+-------------------------------------------------------------------------+-------------------------------------------------+
| Table SQL statement         | String  | The SQL statement used to perform a **free form query**.                | ``INSERT INTO test (col1, col2) VALUES (?, ?)`` |
|                             |         | *Optional*. See note below.                                             |                                                 |
+-----------------------------+---------+-------------------------------------------------------------------------+-------------------------------------------------+
| Table column names          | String  | Columns to insert into the JDBC data source.                            | col1,col2                                       |
|                             |         | *Optional* Comma separated list of columns.                             |                                                 |
+-----------------------------+---------+-------------------------------------------------------------------------+-------------------------------------------------+
| Stage table name            | String  | The name of the table used as a *staging table*.                        | staging                                         |
|                             |         | *Optional*.                                                             |                                                 |
+-----------------------------+---------+-------------------------------------------------------------------------+-------------------------------------------------+
| Should clear stage table    | Boolean | True or false depending on whether the staging table should be cleared  | true                                            |
|                             |         | after the data transfer has finished. *Optional*.                       |                                                 |
+-----------------------------+---------+-------------------------------------------------------------------------+-------------------------------------------------+

**Notes**
=========

1. *Table name* and *Table SQL statement* are mutually exclusive. If *Table name* is provided, the *Table SQL statement* should not be provided. If *Table SQL statement* is provided then *Table name* should not be provided.
2. *Table column names* should be provided only if *Table name* is provided.

-----------
Partitioner
-----------

The Generic JDBC Connector partitioner generates conditions to be used by the extractor.
It varies in how it partitions data transfer based on the partition column data type.
Though, each strategy roughly takes on the following form:
::

  (upper boundary - lower boundary) / (max partitions)

By default, the *primary key* will be used to partition the data unless otherwise specified.

The following data types are currently supported:

1. TINYINT
2. SMALLINT
3. INTEGER
4. BIGINT
5. REAL
6. FLOAT
7. DOUBLE
8. NUMERIC
9. DECIMAL
10. BIT
11. BOOLEAN
12. DATE
13. TIME
14. TIMESTAMP
15. CHAR
16. VARCHAR
17. LONGVARCHAR

---------
Extractor
---------

During the *extraction* phase, the JDBC data source is queried using SQL. This SQL will vary based on your configuration.

- If *Table name* is provided, then the SQL statement generated will take on the form ``SELECT * FROM <table name>``.
- If *Table name* and *Columns* are provided, then the SQL statement generated will take on the form ``SELECT <columns> FROM <table name>``.
- If *Table SQL statement* is provided, then the provided SQL statement will be used.

The conditions generated by the *partitioner* are appended to the end of the SQL query to query a section of data.

The Generic JDBC connector extracts CSV data usable by the *CSV Intermediate Data Format*.

------
Loader
------

During the *loading* phase, the JDBC data source is queried using SQL. This SQL will vary based on your configuration.

- If *Table name* is provided, then the SQL statement generated will take on the form ``INSERT INTO <table name> (col1, col2, ...) VALUES (?,?,..)``.
- If *Table name* and *Columns* are provided, then the SQL statement generated will take on the form ``INSERT INTO <table name> (<columns>) VALUES (?,?,..)``.
- If *Table SQL statement* is provided, then the provided SQL statement will be used.

This connector expects to receive CSV data consumable by the *CSV Intermediate Data Format*.

----------
Destroyers
----------

The Generic JDBC Connector performs two operations in the destroyer in the TO direction:

1. Copy the contents of the staging table to the desired table.
2. Clear the staging table.

No operations are performed in the FROM direction.


++++++++++++++
HDFS Connector
++++++++++++++

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


+++++++++++++++
Kafka Connector
+++++++++++++++

Currently, only the TO direction is supported.

-----
Usage
-----

To use the Kafka Connector, create a link for the connector and a job that uses the link.

**Link Configuration**
++++++++++++++++++++++

Inputs associated with the link configuration include:

+----------------------+---------+-----------------------------------------------------------+-------------------------------------+
| Input                | Type    | Description                                               | Example                             |
+======================+=========+===========================================================+=====================================+
| Broker list          | String  | Comma separated list of kafka brokers.                    | example.com:10000,example.com:11000 |
|                      |         | *Required*.                                               |                                     |
+----------------------+---------+-----------------------------------------------------------+-------------------------------------+
| Zookeeper connection | String  | Comma separated list of zookeeper servers in your quorum. | /etc/conf/hadoop                    |
|                      |         | *Required*.                                               |                                     |
+----------------------+---------+-----------------------------------------------------------+-------------------------------------+

**TO Job Configuration**
++++++++++++++++++++++++

Inputs associated with the Job configuration for the FROM direction include:

+-------+---------+---------------------------------+----------+
| Input | Type    | Description                     | Example  |
+=======+=========+=================================+==========+
| topic | String  | The Kafka topic to transfer to. | my topic |
|       |         | *Required*.                     |          |
+-------+---------+---------------------------------+----------+

------
Loader
------

During the *loading* phase, Kafka is written to directly from each loader. The order in which data is loaded into Kafka is not guaranteed.

++++++++++++++
Kite Connector
++++++++++++++

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

++++++++++++++
SFTP Connector
++++++++++++++

The SFTP connector supports moving data between a Secure File Transfer Protocol (SFTP) server and other supported Sqoop2 connectors.

Currently only the TO direction is supported to write records to an SFTP server. A FROM connector is pending (SQOOP-2218).

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

++++++++++++++
FTP Connector
++++++++++++++

The FTP connector supports moving data between an FTP server and other supported Sqoop2 connectors.

Currently only the TO direction is supported to write records to an FTP server. A FROM connector is pending (SQOOP-2127).

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
