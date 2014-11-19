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

This page will walk you through the basic usage of Sqoop. You need to have installed and configured Sqoop server and client in order to follow this guide. Installation procedure is described on `Installation page <Installation.html>`_. Please note that exact output shown in this page might differ from yours as Sqoop evolves. All major information should however remain the same.

Sqoop uses unique names or persistent ids to identify connectors, links, jobs and configs. We support querying a entity by its unique name or by its perisent database Id.

Starting Client
===============

Start client in interactive mode using following command: ::

  sqoop2-shell

Configure client to use your Sqoop server: ::

  sqoop:000> set server --host your.host.com --port 12000 --webapp sqoop

Verify that connection is working by simple version checking: ::

  sqoop:000> show version --all
  client version:
    Sqoop 2.0.0-SNAPSHOT source revision 418c5f637c3f09b94ea7fc3b0a4610831373a25f
    Compiled by vbasavaraj on Mon Nov  3 08:18:21 PST 2014
  server version:
    Sqoop 2.0.0-SNAPSHOT source revision 418c5f637c3f09b94ea7fc3b0a4610831373a25f
    Compiled by vbasavaraj on Mon Nov  3 08:18:21 PST 2014
  API versions:
    [v1]

You should received similar output as shown above describing the sqoop client build version, the server build version and the supported versions for the rest API.

You can use the help command to check all the supported commands in the sqoop shell.
::

  sqoop:000> help
  For information about Sqoop, visit: http://sqoop.apache.org/

  Available commands:
    exit    (\x  ) Exit the shell
    history (\H  ) Display, manage and recall edit-line history
    help    (\h  ) Display this help message
    set     (\st ) Configure various client options and settings
    show    (\sh ) Display various objects and configuration options
    create  (\cr ) Create new object in Sqoop repository
    delete  (\d  ) Delete existing object in Sqoop repository
    update  (\up ) Update objects in Sqoop repository
    clone   (\cl ) Create new object based on existing one
    start   (\sta) Start job
    stop    (\stp) Stop job
    status  (\stu) Display status of a job
    enable  (\en ) Enable object in Sqoop repository
    disable (\di ) Disable object in Sqoop repository


Creating Link Object
==========================

Check for the registered connectors on your Sqoop server: ::

  sqoop:000> show connector
  +----+------------------------+----------------+------------------------------------------------------+----------------------+
  | Id |          Name          |    Version     |                        Class                         | Supported Directions |
  +----+------------------------+----------------+------------------------------------------------------+----------------------+
  | 1  | hdfs-connector         | 2.0.0-SNAPSHOT | org.apache.sqoop.connector.hdfs.HdfsConnector        | FROM/TO              |
  | 2  | generic-jdbc-connector | 2.0.0-SNAPSHOT | org.apache.sqoop.connector.jdbc.GenericJdbcConnector | FROM/TO              |
  +----+------------------------+----------------+------------------------------------------------------+----------------------+

Our example contains two connectors. The one with connector Id 2 is called the ``generic-jdbc-connector``. This is a basic connector relying on the Java JDBC interface for communicating with data sources. It should work with the most common databases that are providing JDBC drivers. Please note that you must install JDBC drivers separately. They are not bundled in Sqoop due to incompatible licenses.

Generic JDBC Connector in our example has a persistence Id 2 and we will use this value to create new link object for this connector. Note that the link name should be unique.
::

  sqoop:000> create link -c 2
  Creating link for connector with id 2
  Please fill following values to create new link object
  Name: First Link

  Link configuration
  JDBC Driver Class: com.mysql.jdbc.Driver
  JDBC Connection String: jdbc:mysql://mysql.server/database
  Username: sqoop
  Password: *****
  JDBC Connection Properties:
  There are currently 0 values in the map:
  entry#protocol=tcp
  New link was successfully created with validation status OK and persistent id 1

Our new link object was created with assigned id 1.

In the ``show connector -all`` we see that there is a hdfs-connector registered in sqoop with the persistent id 1. Let us create another link object but this time for the  hdfs-connector instead.

::

  sqoop:000> create link -c 1
  Creating link for connector with id 1
  Please fill following values to create new link object
  Name: Second Link

  Link configuration
  HDFS URI: hdfs://nameservice1:8020/
  New link was successfully created with validation status OK and persistent id 2

Creating Job Object
===================

Connectors implement the ``From`` for reading data from and/or ``To`` for writing data to. Generic JDBC Connector supports both of them List of supported directions for each connector might be seen in the output of ``show connector -all`` command above. In order to create a job we need to specifiy the ``From`` and ``To`` parts of the job uniquely identified by their link Ids. We already have 2 links created in the system, you can verify the same with the following command

::

  sqoop:000> show link --all
  2 link(s) to show:
  link with id 1 and name First Link (Enabled: true, Created by root at 11/4/14 4:27 PM, Updated by root at 11/4/14 4:27 PM)
  Using Connector id 2
    Link configuration
      JDBC Driver Class: com.mysql.jdbc.Driver
      JDBC Connection String: jdbc:mysql://mysql.ent.cloudera.com/sqoop
      Username: sqoop
      Password:
      JDBC Connection Properties:
        protocol = tcp
  link with id 2 and name Second Link (Enabled: true, Created by root at 11/4/14 4:38 PM, Updated by root at 11/4/14 4:38 PM)
  Using Connector id 1
    Link configuration
      HDFS URI: hdfs://nameservice1:8020/

Next, we can use the two link Ids to associate the ``From`` and ``To`` for the job.
::

   sqoop:000> create job -f 1 -t 2
   Creating job for links with from id 1 and to id 2
   Please fill following values to create new job object
   Name: Sqoopy

   FromJob configuration

    Schema name:(Required)sqoop
    Table name:(Required)sqoop
    Table SQL statement:(Optional)
    Table column names:(Optional)
    Partition column name:(Optional) id
    Null value allowed for the partition column:(Optional)
    Boundary query:(Optional)

  ToJob configuration

    Output format:
     0 : TEXT_FILE
     1 : SEQUENCE_FILE
    Choose: 0
    Compression format:
     0 : NONE
     1 : DEFAULT
     2 : DEFLATE
     3 : GZIP
     4 : BZIP2
     5 : LZO
     6 : LZ4
     7 : SNAPPY
     8 : CUSTOM
    Choose: 0
    Custom compression format:(Optional)
    Output directory:(Required)/root/projects/sqoop

    Driver Config
    Extractors:(Optional) 2
    Loaders:(Optional) 2
    New job was successfully created with validation status OK  and persistent id 1

Our new job object was created with assigned id 1.

Start Job ( a.k.a Data transfer )
=================================

You can start a sqoop job with the following command:
::

  sqoop:000> start job -j 1
  Submission details
  Job ID: 1
  Server URL: http://localhost:12000/sqoop/
  Created by: root
  Creation date: 2014-11-04 19:43:29 PST
  Lastly updated by: root
  External ID: job_1412137947693_0001
    http://vbsqoop-1.ent.cloudera.com:8088/proxy/application_1412137947693_0001/
  2014-11-04 19:43:29 PST: BOOTING  - Progress is not available

You can iteratively check your running job status with ``status job`` command:

::

  sqoop:000> status job -j 1
  Submission details
  Job ID: 1
  Server URL: http://localhost:12000/sqoop/
  Created by: root
  Creation date: 2014-11-04 19:43:29 PST
  Lastly updated by: root
  External ID: job_1412137947693_0001
    http://vbsqoop-1.ent.cloudera.com:8088/proxy/application_1412137947693_0001/
  2014-11-04 20:09:16 PST: RUNNING  - 0.00 % 

Alternatively you can start a sqoop job and observe job running status with the following command:

::

  sqoop:000> start job -j 1 -s
  Submission details
  Job ID: 1
  Server URL: http://localhost:12000/sqoop/
  Created by: root
  Creation date: 2014-11-04 19:43:29 PST
  Lastly updated by: root
  External ID: job_1412137947693_0001
    http://vbsqoop-1.ent.cloudera.com:8088/proxy/application_1412137947693_0001/
  2014-11-04 19:43:29 PST: BOOTING  - Progress is not available
  2014-11-04 19:43:39 PST: RUNNING  - 0.00 %
  2014-11-04 19:43:49 PST: RUNNING  - 10.00 %

And finally you can stop running the job at any time using ``stop job`` command: ::

  sqoop:000> stop job -j 1