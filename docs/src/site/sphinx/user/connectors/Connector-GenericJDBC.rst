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


======================
Generic JDBC Connector
======================

The Generic JDBC Connector can connect to any data source that adheres to the **JDBC 4** specification.

.. contents::
   :depth: 3

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