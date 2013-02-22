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


====================
Sqoop 5 Minutes Demo
====================

This page will walk you through basic usage of Sqoop. You need to have installed and configured Sqoop server and client in order to follow this guide. Installation procedure is described on `Installation page <Installation.html>`_. Please note that precise output shown in this page might differ from your as Sqoop develops. All major information should however remain the same.

Sqoop is using numerical identifiers to identify various meta data structures (connectors, connections, jobs). Each meta data structures have it's own pool of identifiers and thus it's perfectly valid when Sqoop have connector with id 1, connection with id 1 and job with id 1 at the same time.

Starting Client
===============

Start client in interactive mode using following command: ::

  ./bin/sqoop.sh client

Configure client to use your Sqoop server: ::

  sqoop:000> set server --host your.host.com --port 12000 --webapp sqoop

Verify that connection is working by simple version checking: ::

  sqoop:000> show version --all
  Server version:
    Sqoop 2.0.0-SNAPSHOT revision Unknown
    Compiled by jarcec on Wed Nov 21 16:15:51 PST 2012
  Client version:
    Sqoop 2.0.0-SNAPSHOT revision Unknown
    Compiled by jarcec on Wed Nov 21 16:15:51 PST 2012
  Protocol version:
    [1]

You should received similar output as shown describing versions of both your client and remote server as well as negotiated protocol version.

Creating Connection Object
==========================

Check what connectors are available on your Sqoop server: ::

  sqoop:000> show connector --all
  1 connector(s) to show:
  Connector with id 1:
    Name: generic-jdbc-connector
    Class: org.apache.sqoop.connector.jdbc.GenericJdbcConnector
    Supported job types: [EXPORT, IMPORT]
  ...

Our example contains one connector called ``generic-jdbc-connector``. This is basic connector that is relying on Java JDBC interface for doing data transfers. It should work on most common databases that are providing JDBC drivers. Please note that you must install JDBC drivers separately. They are not bundled in Sqoop due to incompatible licenses.

Generic JDBC Connector have in our example id 1 and we will use this value to create new connection object for this connector: ::

  sqoop:000> create connection --cid 1
  Creating connection for connector with id 1
  Please fill following values to create new connection object
  Name: First connection

  Configuration configuration
  JDBC Driver Class: com.mysql.jdbc.Driver
  JDBC Connection String: jdbc:mysql://mysql.server/database
  Username: sqoop
  Password: *****
  JDBC Connection Properties:
  There are currently 0 values in the map:
  entry#

  Security related configuration options
  Max connections: 0
  New connection was successfully created with validation status FINE and persistent id 1

Our new connection object was created with assigned id 1.

Creating Job Object
===================

Job objects have multiple types and each connector might not support all of them. Generic JDBC Connector supports job types ``import`` (importing data to Hadoop ecosystem) and ``export`` (exporting data from Hadoop ecosystem). List of supported job types for each connector might be seen in the output of ``show connector`` command: ::

  sqoop:000> show connector --all
  ...
    Name: generic-jdbc-connector
  ...
    Supported job types: [EXPORT, IMPORT]
  ...

Create import job for Connection object created in previous section: ::

  sqoop:000> create job --xid 1 --type import
  Creating job for connection with id 1
  Please fill following values to create new job object
  Name: First job

  Database configuration
  Table name: users
  Table SQL statement:
  Table column names:
  Partition column name:
  Boundary query:

  Output configuration
  Storage type:
    0 : HDFS
  Choose: 0
  Output directory: /user/jarcec/users
  New job was successfully created with validation status FINE and persistent id 1

Our new job object was created with assigned id 1.

Moving Data
===========

When all meta data objects are in place we can start moving data around. You can submit Hadoop job using ``submission start`` command: ::

  sqoop:000> submission start --jid 1
  Submission details
  Job id: 1
  Status: BOOTING
  Creation date: 2012-12-23 13:20:34 PST
  Last update date: 2012-12-23 13:20:34 PST
  External Id: job_1353136146286_0004
          http://hadoop.cluster.com:8088/proxy/application_1353136146286_0004/
  Progress: Progress is not available

You can iteratively check your running job status with ``submission status`` command: ::

  sqoop:000> submission status --jid 1
  Submission details
  Job id: 1
  Status: RUNNING
  Creation date: 2012-12-23 13:21:45 PST
  Last update date: 2012-12-23 13:21:56 PST
  External Id: job_1353136146286_0005
          http://hadoop.cluster.com:8088/proxy/application_1353136146286_0004/
  Progress: 0.00 %

And finally you can stop running job at any time using ``submission stop`` command: ::

  sqoop:000> submission stop --jid 1
  Submission details
  Job id: 1
  Status: FAILED
  Creation date: 2012-12-23 13:22:39 PST
  Last update date: 2012-12-23 13:22:42 PST
  External Id: job_1353136146286_0006
          http://hadoop.cluster.com:8088/proxy/application_1353136146286_0004/

