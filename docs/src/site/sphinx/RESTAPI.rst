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

=========================
Sqoop REST API Guide
=========================

This document will explain you how to use Sqoop Network API to allow external applications interacting with Sqoop server.
The REST API is a lower level API than the `Sqoop client API <ClientAPI.html>`_, which gives you the freedom to execute commands in Sqoop
server with any tools or programming language. Generally, the REST API is leveraged via HTTP requests and use ``JSON`` format to encode data content.

.. contents:: Table of Contents

Initialization
=========================

Before making any move, make sure that the Sqoop server is running.

Then find out three pieces of information about the Sqoop server: ``host``, ``port`` and ``webapp``, and keep them in mind.

To execute a function of Sqoop, you could assemble and send a HTTP request to an url of that function in Sqoop. Generally, the url
contain the ``host`` has the hostname, the ``port`` as the port number and ``webapp`` as the root directory of the url, which follows
any functions available in this page.

The request and its response might need to contain some additional parameters and contents. These parameters could be given via
HTTP header, body or both. All the content in the HTTP body is in ``JSON`` format.

Understand Connector, Connection, Job and Framework
===========================================================

To create and run a Sqoop task, we need to provide a bunch of parameters. All these parameters are provide via Input.

Some of these parameters are connector specific, which are needed for a connector. And the other parameters
might be required all the times. Sqoop provides these specifications via forms, each of which is a list of inputs. Each connector claims its own
forms. And the Sqoop framework claims global forms.

On the other hand, some of these parameters are stable for different jobs, like the url, the username and the password of database. And some
of them are job specific. Therefore Sqoop categorizes these parameters into 2 groups, which are connection object and job object. So, for
each connector and the framework, it has to provide forms for connection and job, respectively.

Objects
==============

This section covers all the objects that might exist in the request or response.

Form
----------

Before creating any connection or job, the first thing to do is getting familiar with all forms in the connector and the framework, via Get Connector
and Get Framework HTTP requests.

Each form is structured below:

+------------------+---------------------------------------------------------+
|   Field          | Description                                             |
+==================+=========================================================+
| ``id``           | The id of this form                                     |
+------------------+---------------------------------------------------------+
| ``inputs``       | A array of input fields of this form                    |
+------------------+---------------------------------------------------------+
| ``name``         | The name of this form                                   |
+------------------+---------------------------------------------------------+
| ``type``         | The type of this form (CONNECTION/JOB)                  |
+------------------+---------------------------------------------------------+

A typical form object is showing below:

::

  {
    "id":3,
    "inputs":[
      {
        "id":13,
        "name":"table.schemaName",
        "type":"STRING",
        "size":50,
        "sensitive":false
      }
    ],
    "name":"table",
    "type":"CONNECTION"
  }

Each input object in a form is structured below:

+------------------+---------------------------------------------------------+
|   Field          | Description                                             |
+==================+=========================================================+
| ``id``           | The id of this input                                    |
+------------------+---------------------------------------------------------+
| ``name``         | The name of this input                                  |
+------------------+---------------------------------------------------------+
| ``type``         | The data type of this input field                       |
+------------------+---------------------------------------------------------+
| ``size``         | The length of this input field                          |
+------------------+---------------------------------------------------------+
| ``sensitive``    | Whether this input contain sensitive information        |
+------------------+---------------------------------------------------------+

The connector and framework have both ``job-forms`` and ``conn-forms``, each of which is a array of forms. In ``job-forms``,
there are 2 arrays of forms, for ``IMPORT`` and ``EXPORT``, job respectively.

To send a filled form in the request, you should always use form id and input id to map the values to inputs. For example, the
following request contains a input value ``com.mysql.jdbc.Driver`` for input ``1`` in form ``1`` of connector and an input value
``10`` for input ``17`` in form ``4`` of framework.

::

  {
    "connector":[
      {
        "id":1,
        "inputs":[
          {
            "id":1,
            "name":"connection.jdbcDriver",
            "value":"com.mysql.jdbc.Driver",
            "type":"STRING",
            "size":128,
            "sensitive":false
          },
        ],
        "name":"connection",
        "type":"CONNECTION"
      }
    ],
    "connector-id":1,
    "framework":[
      {
        "id":4,
        "inputs":[
          {
            "id":17,
            "name":"security.maxConnections",
            "value":"10",
            "type":"INTEGER",
            "sensitive":false
          }
        ],
        "name":"security",
        "type":"CONNECTION"
      }
    ]
  }

Exception
---------------

Each operation on Sqoop server might return an exception in the Http response. Remember to take this into account.

The exception code and message could be found in both the header and body of the response, if happens.

Please jump to "Header Parameters" section to find how to get exception information from header.

In the body, the exception is expressed in ``JSON`` format. An example of the exception is:

::

  {
    "message":"DERBYREPO_0030:Unable to load specific job metadata from repository - Couldn't find job with id 2",
    "stack-trace":[
      {
        "file":"DerbyRepositoryHandler.java",
        "line":1111,
        "class":"org.apache.sqoop.repository.derby.DerbyRepositoryHandler",
        "method":"findJob"
      },
      {
        "file":"JdbcRepository.java",
        "line":451,
        "class":"org.apache.sqoop.repository.JdbcRepository$16",
        "method":"doIt"
      },
      {
        "file":"JdbcRepository.java",
        "line":90,
        "class":"org.apache.sqoop.repository.JdbcRepository",
        "method":"doWithConnection"
      },
      {
        "file":"JdbcRepository.java",
        "line":61,
        "class":"org.apache.sqoop.repository.JdbcRepository",
        "method":"doWithConnection"
      },
      {
        "file":"JdbcRepository.java",
        "line":448,
        "class":"org.apache.sqoop.repository.JdbcRepository",
        "method":"findJob"
      },
      {
        "file":"JobRequestHandler.java",
        "line":238,
        "class":"org.apache.sqoop.handler.JobRequestHandler",
        "method":"getJobs"
      }
    ],
    "class":"org.apache.sqoop.common.SqoopException"
  }

Form Validation Status
---------------------------

After submitting the forms of creating/updating connection/job, the server will validate these forms and send
back feedbacks to show the validation status.

There are 3 possible status:

+------------------+---------------------------------------------------------+
|   Status         | Description                                             |
+==================+=========================================================+
| ``FINE``         | No issues, no warnings. Everything is perfect           |
+------------------+---------------------------------------------------------+
| ``ACCEPTABLE``   | No issues, but might be some warnings. Good to go.      |
+------------------+---------------------------------------------------------+
| ``UNACCEPTABLE`` | The form has severe issues needed to be fixed           |
+------------------+---------------------------------------------------------+

An example of a good status is:

::

  {
   "status":"FINE",
   "messages":{}
  }

A bad status might be:

::

  {
    "message":"Can't load specified driver",
    "status":"UNACCEPTABLE"
  }

Job Status
-------------------

After submitting a job, you could look up the running status of it. There could be 7 possible status:

+-----------------------------+---------------------------------------------------------+
|   Status                    | Description                                             |
+=============================+=========================================================+
| ``BOOTING``                 | In the middle of submitting the job                     |
+-----------------------------+---------------------------------------------------------+
| ``FAILURE_ON_SUBMIT``       | Unable to submit this job to remote cluster             |
+-----------------------------+---------------------------------------------------------+
| ``RUNNING``                 | The job is running now                                  |
+-----------------------------+---------------------------------------------------------+
| ``SUCCEEDED``               | Job finished successfully                               |
+-----------------------------+---------------------------------------------------------+
| ``FAILED``                  | Job failed                                              |
+-----------------------------+---------------------------------------------------------+
| ``NEVER_EXECUTED``          | The job has never been executed since created           |
+-----------------------------+---------------------------------------------------------+
| ``UNKNOWN``                 | The status is unknown                                   |
+-----------------------------+---------------------------------------------------------+

Header Parameters
======================

For all Sqoop requests, the following header parameters are supported:

+---------------------------+----------+---------------------------------------------------------+
|   Parameter               | Required | Description                                             |
+===========================+==========+=========================================================+
| ``sqoop-user-name``       | true     | The name of the user who makes the requests             |
+---------------------------+----------+---------------------------------------------------------+

For all the responses, the following parameters in the HTTP message header are available:

+---------------------------+----------+------------------------------------------------------------------------------+
|   Parameter               | Required | Description                                                                  |
+===========================+==========+==============================================================================+
| ``sqoop-error-code``      | false    | The error code when some error happen in the server side for this request    |
+---------------------------+----------+------------------------------------------------------------------------------+
| ``sqoop-error-message``   | false    | The explanation for a error code                                             |
+---------------------------+----------+------------------------------------------------------------------------------+

So far, there are only these 2 parameters in the header of response message. They only exist when something bad happen in the server.
And they always come along with an exception message in the response body.

Functions
==================

The section elaborates all the functions that are supported by the Sqoop server.

/version - [GET] - Get Sqoop Version
-------------------------------------------

Get all the version metadata of Sqoop software in the server side.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Fields of Response:

+---------------+---------------------------------------------------------+
|   Field       | Description                                             |
+===============+=========================================================+
| ``revision``  | The revision number of Sqoop source code                |
+---------------+---------------------------------------------------------+
| ``protocols`` | The version of network protocol                         |
+---------------+---------------------------------------------------------+
| ``date``      | The Sqoop release date                                  |
+---------------+---------------------------------------------------------+
| ``user``      | The user who made the release                           |
+---------------+---------------------------------------------------------+
| ``url``       | The url of the source code trunk                        |
+---------------+---------------------------------------------------------+
| ``version``   | The version of Sqoop in the server side                 |
+---------------+---------------------------------------------------------+


* Response Example:

::

  {
    "revision":"e56c977b56f4dc32a4cad06a328bad11e0d0055b",
    "protocols":["1"],
    "date":"Wed Aug  7 13:31:36 PDT 2013",
    "user":"mengwei.ding",
    "url":"git:\/\/mding-MBP.local\/Users\/mengwei.ding\/Documents\/workspace\/sqoop2\/common",
    "version":"2.0.0-SNAPSHOT"
  }

/v1/connector/[cid] - [GET] - Get Connector
---------------------------------------------------

Retrieve all the metadata of a given connector, such as its forms to be filled for jobs and connections, the explanation
for each fields of these forms.

Provide the id of the connector in the url ``[cid]`` part. If you provide ``all`` in the ``[cid]`` part in the url, you will
get the metadata of all connectors.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Fields of Response:

+--------------------------+--------------------------------------------------------------------------+
|   Field                  | Description                                                              |
+==========================+==========================================================================+
| ``resources-connector``  | All resources for the given connector                                    |
+--------------------------+--------------------------------------------------------------------------+
| ``all``                  | All metadata about the given connector, such as id, name and all forms   |
+--------------------------+--------------------------------------------------------------------------+

If all connectors are retrieved, the ``resources-connector`` and ``all`` fields will become arrays and contain data for all connectors.

So far, the resource contains only explanations for fields of forms. For example, in the IMPORT job form, you could find a field called
``table.schemaName``. If you have no idea about what that field means, you could go the resource for help.

* Response Example:

::

  {
    "resources-connector":{
      "1":{
        "ignored.label":"Ignored",
        "table.partitionColumn.help":"A specific column for data partition",
        "table.label":"Database configuration",
        "table.boundaryQuery.label":"Boundary query",
        "ignored.help":"This is completely ignored",
        "ignored.ignored.label":"Ignored",
        "connection.jdbcProperties.help":"Enter any JDBC properties that should be supplied during the creation of connection.",
        "table.tableName.help":"Table name to process data in the remote database",
        "connection.jdbcDriver.label":"JDBC Driver Class",
        "connection.username.help":"Enter the username to be used for connecting to the database.",
        "table.help":"You must supply the information requested in order to create a job object.",
        "table.partitionColumn.label":"Partition column name",
        "ignored.ignored.help":"This is completely ignored",
        "table.partitionColumnNull.label":"Nulls in partition column",
        "table.warehouse.label":"Data warehouse",
        "table.boundaryQuery.help":"The boundary query for data partition",
        "connection.username.label":"Username",
        "connection.jdbcDriver.help":"Enter the fully qualified class name of the JDBC driver that will be used for establishing this connection.",
        "connection.label":"Connection configuration",
        "table.columns.label":"Table column names",
        "table.dataDirectory.label":"Data directory",
        "table.partitionColumnNull.help":"Whether there are null values in partition column",
        "connection.password.label":"Password",
        "table.warehouse.help":"The root directory for data",
        "table.sql.label":"Table SQL statement",
        "table.sql.help":"SQL statement to process data in the remote database",
        "table.schemaName.help":"Schema name to process data in the remote database",
        "connection.jdbcProperties.label":"JDBC Connection Properties",
        "table.columns.help":"Specific columns of a table name or a table SQL",
        "connection.connectionString.help":"Enter the value of JDBC connection string to be used by this connector for creating connections.",
        "table.dataDirectory.help":"The sub-directory under warehouse for data",
        "table.schemaName.label":"Schema name",
        "connection.connectionString.label":"JDBC Connection String",
        "connection.help":"You must supply the information requested in order to create a connection object.",
        "connection.password.help":"Enter the password to be used for connecting to the database.",
        "table.tableName.label":"Table name"
      }
    },
    "all":[
      {
        "id":1,
        "name":"generic-jdbc-connector",
        "class":"org.apache.sqoop.connector.jdbc.GenericJdbcConnector",
        "job-forms":{
          "IMPORT":[
            {
              "id":2,
              "inputs":[
                {
                  "id":6,
                  "name":"table.schemaName",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                },
                {
                  "id":7,
                  "name":"table.tableName",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                },
                {
                  "id":8,
                  "name":"table.sql",
                  "type":"STRING",
                  "size":2000,
                  "sensitive":false
                },
                {
                  "id":9,
                  "name":"table.columns",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                },
                {
                  "id":10,
                  "name":"table.partitionColumn",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                },
                {
                  "id":11,
                  "name":"table.partitionColumnNull",
                  "type":"BOOLEAN",
                  "sensitive":false
                },
                {
                  "id":12,
                  "name":"table.boundaryQuery",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                }
              ],
              "name":"table",
              "type":"CONNECTION"
            }
          ],
          "EXPORT":[
            {
              "id":3,
              "inputs":[
                {
                  "id":13,
                  "name":"table.schemaName",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                },
                {
                  "id":14,
                  "name":"table.tableName",
                  "type":"STRING",
                  "size":2000,
                  "sensitive":false
                },
                {
                  "id":15,
                  "name":"table.sql",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                },
                {
                  "id":16,
                  "name":"table.columns",
                  "type":"STRING",
                  "size":50,
                  "sensitive":false
                }
              ],
              "name":"table",
              "type":"CONNECTION"
            }
          ]
        },
        "con-forms":[
          {
            "id":1,
            "inputs":[
              {
                "id":1,
                "name":"connection.jdbcDriver",
                "type":"STRING",
                "size":128,
                "sensitive":false
              },
              {
                "id":2,
                "name":"connection.connectionString",
                "type":"STRING",
                "size":128,
                "sensitive":false
              },
              {
                "id":3,
                "name":"connection.username",
                "type":"STRING",
                "size":40,
                "sensitive":false
              },
              {
                "id":4,
                "name":"connection.password",
                "type":"STRING",
                "size":40,
                "sensitive":true
              },
              {
                "id":5,
                "name":"connection.jdbcProperties",
                "type":"MAP",
                "sensitive":false
              }
            ],
            "name":"connection",
            "type":"CONNECTION"
          }
        ],
        "version":"2.0.0-SNAPSHOT"
      }
    ]
  }


/v1/framework - [GET]- Get Sqoop Framework
-----------------------------------------------

Retrieve all metadata of Sqoop framework. The metadata include all the form fields that are required to all Sqoop objects, such as connection and jobs.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Fields of Response:

+--------------------------+----------------------------------------------------------------------------------------------------+
|   Field                  | Description                                                                                        |
+==========================+====================================================================================================+
| ``id``                   | The id for Sqoop framework (It should be always be 1, since there is always 1 framework out there) |
+--------------------------+----------------------------------------------------------------------------------------------------+
| ``resources``            | All resources for Sqoop framework                                                                  |
+--------------------------+----------------------------------------------------------------------------------------------------+
| ``framework-version``    | The version of Sqoop framework                                                                     |
+--------------------------+----------------------------------------------------------------------------------------------------+
| ``job-forms``            | Framework's Job Configuration forms                                                                |
+--------------------------+----------------------------------------------------------------------------------------------------+
| ``con-forms``            | Framework's connection configuration forms                                                         |
+--------------------------+----------------------------------------------------------------------------------------------------+

The framework and connector might contain several forms to be filled for job or connection object. Many parameters for job and connection
are categorize into different classes, which are know as forms. Each form has its id and name. In job and connection objects, they use
the id of the form to track these parameter inputs.

* Response Example:

::

  {
    "id":1,
    "resources":{
      "output.label":"Output configuration",
      "security.maxConnections.help":"Maximal number of connections that this connection object can use at one point in time",
      "output.storageType.label":"Storage type",
      "output.ignored.help":"This value is ignored",
      "input.label":"Input configuration",
      "security.help":"You must supply the information requested in order to create a job object.",
      "output.storageType.help":"Target on Hadoop ecosystem where to store data",
      "input.inputDirectory.help":"Directory that should be exported",
      "output.outputFormat.label":"Output format",
      "output.ignored.label":"Ignored",
      "output.outputFormat.help":"Format in which data should be serialized",
      "output.help":"You must supply the information requested in order to get information where you want to store your data.",
      "throttling.help":"Set throttling boundaries to not overload your systems",
      "input.inputDirectory.label":"Input directory",
      "throttling.loaders.label":"Loaders",
      "input.help":"Specifies information required to get data from Hadoop ecosystem",
      "throttling.extractors.label":"Extractors",
      "throttling.extractors.help":"Number of extractors that Sqoop will use",
      "security.label":"Security related configuration options",
      "throttling.label":"Throttling resources",
      "throttling.loaders.help":"Number of loaders that Sqoop will use",
      "output.outputDirectory.help":"Output directory for final data",
      "security.maxConnections.label":"Max connections",
      "output.outputDirectory.label":"Output directory"
    },
    "framework-version":"1",
    "job-forms":{
      "IMPORT":[
        {
          "id":5,
          "inputs":[
            {
              "id":18,
              "values":"HDFS",
              "name":"output.storageType",
              "type":"ENUM",
              "sensitive":false
            },
            {
              "id":19,
              "values":"TEXT_FILE,SEQUENCE_FILE",
              "name":"output.outputFormat",
              "type":"ENUM",
              "sensitive":false
            },
            {
              "id":20,
              "name":"output.outputDirectory",
              "type":"STRING",
              "size":255,
              "sensitive":false
            }
          ],
          "name":"output",
          "type":"CONNECTION"
        },
        {
          "id":6,
          "inputs":[
            {
              "id":21,
              "name":"throttling.extractors",
              "type":"INTEGER",
              "sensitive":false
            },
            {
              "id":22,
              "name":"throttling.loaders",
              "type":"INTEGER",
              "sensitive":false
            }
          ],
          "name":"throttling",
          "type":"CONNECTION"
        }
      ],
      "EXPORT":[
        {
          "id":7,
          "inputs":[
            {
              "id":23,
              "name":"input.inputDirectory",
              "type":"STRING",
              "size":255,
              "sensitive":false
            }
          ],
          "name":"input",
          "type":"CONNECTION"
        },
        {
          "id":8,
          "inputs":[
            {
              "id":24,
              "name":"throttling.extractors",
              "type":"INTEGER",
              "sensitive":false
            },
            {
              "id":25,
              "name":"throttling.loaders",
              "type":"INTEGER",
              "sensitive":false
            }
          ],
          "name":"throttling",
          "type":"CONNECTION"
        }
      ]
    },
    "con-forms":[
      {
        "id":4,
        "inputs":[
          {
            "id":17,
            "name":"security.maxConnections",
            "type":"INTEGER",
            "sensitive":false
          }
        ],
        "name":"security",
        "type":"CONNECTION"
      }
    ]
  }

/v1/connection/[xid] - [GET] - Get Connection
----------------------------------------------------

Retrieve all the metadata of a given connection, such as its values for different fields of connector form and sqoop framework form.

Provide the id of the connector in the url [xid] part. If you provide ``all`` in the [xid] part in the url, you will get the metadata of all connections.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Fields of Response:

+--------------------------+---------------------------------------------------------------------------------------+
|   Field                  | Description                                                                           |
+==========================+=======================================================================================+
| ``resources-connector``  | All resources for the given connector                                                 |
+--------------------------+---------------------------------------------------------------------------------------+
| ``resources-framework``  | All resources related with Sqoop framework                                            |
+--------------------------+---------------------------------------------------------------------------------------+
| ``all``                  | All metadata about the given connection, such as id, name and all form input values   |
+--------------------------+---------------------------------------------------------------------------------------+

* Response Example:

::

  {
    "resources-connector":{
      "1":{
        "ignored.label":"Ignored",
        "table.partitionColumn.help":"A specific column for data partition",
        "table.label":"Database configuration",
        "table.boundaryQuery.label":"Boundary query",
        "ignored.help":"This is completely ignored",
        "ignored.ignored.label":"Ignored",
        "connection.jdbcProperties.help":"Enter any JDBC properties that should be supplied during the creation of connection.",
        "table.tableName.help":"Table name to process data in the remote database",
        "connection.jdbcDriver.label":"JDBC Driver Class",
        "connection.username.help":"Enter the username to be used for connecting to the database.",
        "table.help":"You must supply the information requested in order to create a job object.",
        "table.partitionColumn.label":"Partition column name",
        "ignored.ignored.help":"This is completely ignored",
        "table.partitionColumnNull.label":"Nulls in partition column",
        "table.warehouse.label":"Data warehouse",
        "table.boundaryQuery.help":"The boundary query for data partition",
        "connection.username.label":"Username",
        "connection.jdbcDriver.help":"Enter the fully qualified class name of the JDBC driver that will be used for establishing this connection.",
        "connection.label":"Connection configuration",
        "table.columns.label":"Table column names",
        "table.dataDirectory.label":"Data directory",
        "table.partitionColumnNull.help":"Whether there are null values in partition column",
        "connection.password.label":"Password",
        "table.warehouse.help":"The root directory for data",
        "table.sql.label":"Table SQL statement",
        "table.sql.help":"SQL statement to process data in the remote database",
        "table.schemaName.help":"Schema name to process data in the remote database",
        "connection.jdbcProperties.label":"JDBC Connection Properties",
        "table.columns.help":"Specific columns of a table name or a table SQL",
        "connection.connectionString.help":"Enter the value of JDBC connection string to be used by this connector for creating connections.",
        "table.dataDirectory.help":"The sub-directory under warehouse for data",
        "table.schemaName.label":"Schema name",
        "connection.connectionString.label":"JDBC Connection String",
        "connection.help":"You must supply the information requested in order to create a connection object.",
        "connection.password.help":"Enter the password to be used for connecting to the database.",
        "table.tableName.label":"Table name"
      }
    },
    "resources-framework":{
      "output.label":"Output configuration",
      "security.maxConnections.help":"Maximal number of connections that this connection object can use at one point in time",
      "output.storageType.label":"Storage type",
      "output.ignored.help":"This value is ignored",
      "input.label":"Input configuration",
      "security.help":"You must supply the information requested in order to create a job object.",
      "output.storageType.help":"Target on Hadoop ecosystem where to store data",
      "input.inputDirectory.help":"Directory that should be exported",
      "output.outputFormat.label":"Output format",
      "output.ignored.label":"Ignored",
      "output.outputFormat.help":"Format in which data should be serialized",
      "output.help":"You must supply the information requested in order to get information where you want to store your data.",
      "throttling.help":"Set throttling boundaries to not overload your systems",
      "input.inputDirectory.label":"Input directory",
      "throttling.loaders.label":"Loaders",
      "input.help":"Specifies information required to get data from Hadoop ecosystem",
      "throttling.extractors.label":"Extractors",
      "throttling.extractors.help":"Number of extractors that Sqoop will use",
      "security.label":"Security related configuration options",
      "throttling.label":"Throttling resources",
      "throttling.loaders.help":"Number of loaders that Sqoop will use",
      "output.outputDirectory.help":"Output directory for final data",
      "security.maxConnections.label":"Max connections",
      "output.outputDirectory.label":"Output directory"
    },
    "all":[
      {
        "id":1,
        "enabled":true,
        "updated":1375912819893,
        "created":1375912819893,
        "name":"First connection",
        "connector":[
          {
            "id":1,
            "inputs":[
              {
                "id":1,
                "name":"connection.jdbcDriver",
                "value":"com.mysql.jdbc.Driver",
                "type":"STRING",
                "size":128,
                "sensitive":false
              },
              {
                "id":2,
                "name":"connection.connectionString",
                "value":"jdbc%3Amysql%3A%2F%2Flocalhost%2Ftest",
                "type":"STRING",
                "size":128,
                "sensitive":false
              },
              {
                "id":3,
                "name":"connection.username",
                "value":"root",
                "type":"STRING",
                "size":40,
                "sensitive":false
              },
              {
                "id":4,
                "name":"connection.password",
                "type":"STRING",
                "size":40,
                "sensitive":true
              },
              {
                "id":5,
                "name":"connection.jdbcProperties",
                "type":"MAP",
                "sensitive":false
              }
            ],
            "name":"connection",
            "type":"CONNECTION"
          }
        ],
        "connector-id":1,
        "framework":[
          {
            "id":4,
            "inputs":[
              {
                "id":17,
                "name":"security.maxConnections",
                "value":"10",
                "type":"INTEGER",
                "sensitive":false
              }
            ],
            "name":"security",
            "type":"CONNECTION"
          }
        ]
      }
    ]
  }

/v1/connection - [POST] - Create Connection
---------------------------------------------------------

Create a new connection object. Try your best to provide values for as many as inputs of
connection forms from both connectors and framework.

* Method: ``POST``
* Format: ``JSON``
* Fields of Request:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``all``                  | Request array, in which each element is an independent request                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``id``                   | The id of the connection. Useless here, because we don't know the id before creation |
+--------------------------+--------------------------------------------------------------------------------------+
| ``enabled``              | Whether to enable this connection (true/false)                                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``updated``              | The last updated time of this connection                                             |
+--------------------------+--------------------------------------------------------------------------------------+
| ``created``              | The creation time of this connection                                                 |
+--------------------------+--------------------------------------------------------------------------------------+
| ``name``                 | The name of this connection                                                          |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connector``            | Filled inputs for connector forms for this connection                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connector-id``         | The id of the connector used for this connection                                     |
+--------------------------+--------------------------------------------------------------------------------------+
| ``framework``            | Filled inputs for framework forms for this connection                                |
+--------------------------+--------------------------------------------------------------------------------------+


* Request Example:

::

  {
    "all":[
      {
        "id":-1,
        "enabled":true,
        "updated":1375919952017,
        "created":1375919952017,
        "name":"First connection",
        "connector":[
          {
            "id":1,
            "inputs":[
              {
                "id":1,
                "name":"connection.jdbcDriver",
                "value":"com.mysql.jdbc.Driver",
                "type":"STRING",
                "size":128,
                "sensitive":false
              },
              {
                "id":2,
                "name":"connection.connectionString",
                "value":"jdbc%3Amysql%3A%2F%2Flocalhost%2Ftest",
                "type":"STRING",
                "size":128,
                "sensitive":false
              },
              {
                "id":3,
                "name":"connection.username",
                "value":"root",
                "type":"STRING",
                "size":40,
                "sensitive":false
              },
              {
                "id":4,
                "name":"connection.password",
                "type":"STRING",
                "size":40,
                "sensitive":true
              },
              {
                "id":5,
                "name":"connection.jdbcProperties",
                "type":"MAP",
                "sensitive":false
              }
            ],
            "name":"connection",
            "type":"CONNECTION"
          }
        ],
        "connector-id":1,
        "framework":[
          {
            "id":4,
            "inputs":[
              {
                "id":17,
                "name":"security.maxConnections",
                "value":"10",
                "type":"INTEGER",
                "sensitive":false
              }
            ],
            "name":"security",
            "type":"CONNECTION"
          }
        ]
      }
    ]
  }

* Fields of Response:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``id``                   | The id assigned for this new created connection                                      |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connector``            | The validation status for the inputs of connector forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+
| ``framework``            | The validation status for the inputs of connector forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+

* Response Example:

::

  {
    "id":1,
    "connector":{
      "status":"FINE",
      "messages":{

      }
    },
    "framework":{
      "status":"FINE",
      "messages":{

      }
    }
  }

/v1/connection/[xid] - [PUT] - Update Connection
---------------------------------------------------------

Update an existing connection object with id [xid]. To make the procedure of filling inputs easier, the general practice
is get the connection first and then change some of the inputs.

* Method: ``PUT``
* Format: ``JSON``
* Fields of Request:

The same as Create Connection.

* Fields of Response:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``connector``            | The validation status for the inputs of connector forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+
| ``framework``            | The validation status for the inputs of connector forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+

* Response Example:

::

  {
    "connector":{
      "status":"FINE",
      "messages":{

      }
    },
    "framework":{
      "status":"FINE",
      "messages":{

      }
    }
  }

/v1/connection/[xid] - [DELETE] - Delete Connection
---------------------------------------------------------

Delete a connection with id ``xid``.

* Method: ``DELETE``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/connection/[xid]/enable - [PUT] - Enable Connection
---------------------------------------------------------

Enable a connection with id ``xid``.

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/connection/[xid]/disable - [PUT] - Disable Connection
---------------------------------------------------------

Disable a connection with id ``xid``.

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/job/[jid] - [GET] - Get Job
----------------------------------------

Retrieve all the metadata of a given job, such as its values for different fields of connector form and sqoop framework form.

Provide the id of the job in the url [jid] part. If you provide ``all`` in the [jid] part in the url, you will get the metadata of all connections.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Fields of Response:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``resources-connector``  | All resources for the given connector                                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``resources-framework``  | All resources related with Sqoop framework                                           |
+--------------------------+--------------------------------------------------------------------------------------+
| ``all``                  | All metadata about the given job, such as id, name and all form input values         |
+--------------------------+--------------------------------------------------------------------------------------+

* Response Example:

::

  {
    "resources-connector":{
      "1":{
        "ignored.label":"Ignored",
        "table.partitionColumn.help":"A specific column for data partition",
        "table.label":"Database configuration",
        "table.boundaryQuery.label":"Boundary query",
        "ignored.help":"This is completely ignored",
        "ignored.ignored.label":"Ignored",
        "connection.jdbcProperties.help":"Enter any JDBC properties that should be supplied during the creation of connection.",
        "table.tableName.help":"Table name to process data in the remote database",
        "connection.jdbcDriver.label":"JDBC Driver Class",
        "connection.username.help":"Enter the username to be used for connecting to the database.",
        "table.help":"You must supply the information requested in order to create a job object.",
        "table.partitionColumn.label":"Partition column name",
        "ignored.ignored.help":"This is completely ignored",
        "table.partitionColumnNull.label":"Nulls in partition column",
        "table.warehouse.label":"Data warehouse",
        "table.boundaryQuery.help":"The boundary query for data partition",
        "connection.username.label":"Username",
        "connection.jdbcDriver.help":"Enter the fully qualified class name of the JDBC driver that will be used for establishing this connection.",
        "connection.label":"Connection configuration",
        "table.columns.label":"Table column names",
        "table.dataDirectory.label":"Data directory",
        "table.partitionColumnNull.help":"Whether there are null values in partition column",
        "connection.password.label":"Password",
        "table.warehouse.help":"The root directory for data",
        "table.sql.label":"Table SQL statement",
        "table.sql.help":"SQL statement to process data in the remote database",
        "table.schemaName.help":"Schema name to process data in the remote database",
        "connection.jdbcProperties.label":"JDBC Connection Properties",
        "table.columns.help":"Specific columns of a table name or a table SQL",
        "connection.connectionString.help":"Enter the value of JDBC connection string to be used by this connector for creating connections.",
        "table.dataDirectory.help":"The sub-directory under warehouse for data",
        "table.schemaName.label":"Schema name",
        "connection.connectionString.label":"JDBC Connection String",
        "connection.help":"You must supply the information requested in order to create a connection object.",
        "connection.password.help":"Enter the password to be used for connecting to the database.",
        "table.tableName.label":"Table name"
      }
    },
    "resources-framework":{
      "output.label":"Output configuration",
      "security.maxConnections.help":"Maximal number of connections that this connection object can use at one point in time",
      "output.storageType.label":"Storage type",
      "output.ignored.help":"This value is ignored",
      "input.label":"Input configuration",
      "security.help":"You must supply the information requested in order to create a job object.",
      "output.storageType.help":"Target on Hadoop ecosystem where to store data",
      "input.inputDirectory.help":"Directory that should be exported",
      "output.outputFormat.label":"Output format",
      "output.ignored.label":"Ignored",
      "output.outputFormat.help":"Format in which data should be serialized",
      "output.help":"You must supply the information requested in order to get information where you want to store your data.",
      "throttling.help":"Set throttling boundaries to not overload your systems",
      "input.inputDirectory.label":"Input directory",
      "throttling.loaders.label":"Loaders",
      "input.help":"Specifies information required to get data from Hadoop ecosystem",
      "throttling.extractors.label":"Extractors",
      "throttling.extractors.help":"Number of extractors that Sqoop will use",
      "security.label":"Security related configuration options",
      "throttling.label":"Throttling resources",
      "throttling.loaders.help":"Number of loaders that Sqoop will use",
      "output.outputDirectory.help":"Output directory for final data",
      "security.maxConnections.label":"Max connections",
      "output.outputDirectory.label":"Output directory"
    },
    "all":[
      {
        "connection-id":1,
        "id":1,
        "enabled":true,
        "updated":1375913231253,
        "created":1375913231253,
        "name":"First job",
        "connector":[
          {
            "id":2,
            "inputs":[
              {
                "id":6,
                "name":"table.schemaName",
                "value":"test",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":7,
                "name":"table.tableName",
                "value":"example",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":8,
                "name":"table.sql",
                "type":"STRING",
                "size":2000,
                "sensitive":false
              },
              {
                "id":9,
                "name":"table.columns",
                "value":"id%2Cdata",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":10,
                "name":"table.partitionColumn",
                "value":"id",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":11,
                "name":"table.partitionColumnNull",
                "value":"true",
                "type":"BOOLEAN",
                "sensitive":false
              },
              {
                "id":12,
                "name":"table.boundaryQuery",
                "type":"STRING",
                "size":50,
                "sensitive":false
              }
            ],
            "name":"table",
            "type":"CONNECTION"
          }
        ],
        "connector-id":1,
        "type":"IMPORT",
        "framework":[
          {
            "id":5,
            "inputs":[
              {
                "id":18,
                "values":"HDFS",
                "name":"output.storageType",
                "value":"HDFS",
                "type":"ENUM",
                "sensitive":false
              },
              {
                "id":19,
                "values":"TEXT_FILE,SEQUENCE_FILE",
                "name":"output.outputFormat",
                "value":"TEXT_FILE",
                "type":"ENUM",
                "sensitive":false
              },
              {
                "id":20,
                "name":"output.outputDirectory",
                "value":"%2Ftmp%2Foutput",
                "type":"STRING",
                "size":255,
                "sensitive":false
              }
            ],
            "name":"output",
            "type":"CONNECTION"
          },
          {
            "id":6,
            "inputs":[
              {
                "id":21,
                "name":"throttling.extractors",
                "type":"INTEGER",
                "sensitive":false
              },
              {
                "id":22,
                "name":"throttling.loaders",
                "type":"INTEGER",
                "sensitive":false
              }
            ],
            "name":"throttling",
            "type":"CONNECTION"
          }
        ]
      }
    ]
  }

/v1/job - [POST] - Create Job
---------------------------------------------------------

Create a new job object. Try your best to provide values for as many as inputs of
job forms from both connectors and framework.

* Method: ``POST``
* Format: ``JSON``
* Fields of Request:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``all``                  | Request array, in which each element is an independent request                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connection-id``        | The id of the connection used for this job                                           |
+--------------------------+--------------------------------------------------------------------------------------+
| ``id``                   | The id of the job. Useless here, because we don't know the id before creation        |
+--------------------------+--------------------------------------------------------------------------------------+
| ``type``                 | The type of this job ("IMPORT"/"EXPORT")                                             |
+--------------------------+--------------------------------------------------------------------------------------+
| ``enabled``              | Whether to enable this connection (true/false)                                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``updated``              | The last updated time of this connection                                             |
+--------------------------+--------------------------------------------------------------------------------------+
| ``created``              | The creation time of this connection                                                 |
+--------------------------+--------------------------------------------------------------------------------------+
| ``name``                 | The name of this connection                                                          |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connector``            | Filled inputs for connector forms for this connection                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connector-id``         | The id of the connector used for this connection                                     |
+--------------------------+--------------------------------------------------------------------------------------+
| ``framework``            | Filled inputs for framework forms for this connection                                |
+--------------------------+--------------------------------------------------------------------------------------+


* Request Example:

::

  {
    "all":[
      {
        "connection-id":1,
        "id":-1,
        "enabled":true,
        "updated":1375920083970,
        "created":1375920083970,
        "name":"First job",
        "connector":[
          {
            "id":2,
            "inputs":[
              {
                "id":6,
                "name":"table.schemaName",
                "value":"test",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":7,
                "name":"table.tableName",
                "value":"example",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":8,
                "name":"table.sql",
                "type":"STRING",
                "size":2000,
                "sensitive":false
              },
              {
                "id":9,
                "name":"table.columns",
                "value":"id%2Cdata",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":10,
                "name":"table.partitionColumn",
                "value":"id",
                "type":"STRING",
                "size":50,
                "sensitive":false
              },
              {
                "id":11,
                "name":"table.partitionColumnNull",
                "value":"true",
                "type":"BOOLEAN",
                "sensitive":false
              },
              {
                "id":12,
                "name":"table.boundaryQuery",
                "type":"STRING",
                "size":50,
                "sensitive":false
              }
            ],
            "name":"table",
            "type":"CONNECTION"
          }
        ],
        "connector-id":1,
        "type":"IMPORT",
        "framework":[
          {
            "id":5,
            "inputs":[
              {
                "id":18,
                "values":"HDFS",
                "name":"output.storageType",
                "value":"HDFS",
                "type":"ENUM",
                "sensitive":false
              },
              {
                "id":19,
                "values":"TEXT_FILE,SEQUENCE_FILE",
                "name":"output.outputFormat",
                "value":"TEXT_FILE",
                "type":"ENUM",
                "sensitive":false
              },
              {
                "id":20,
                "name":"output.outputDirectory",
                "value":"%2Ftmp%2Foutput",
                "type":"STRING",
                "size":255,
                "sensitive":false
              }
            ],
            "name":"output",
            "type":"CONNECTION"
          },
          {
            "id":6,
            "inputs":[
              {
                "id":21,
                "name":"throttling.extractors",
                "type":"INTEGER",
                "sensitive":false
              },
              {
                "id":22,
                "name":"throttling.loaders",
                "type":"INTEGER",
                "sensitive":false
              }
            ],
            "name":"throttling",
            "type":"CONNECTION"
          }
        ]
      }
    ]
  }

* Fields of Response:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``id``                   | The id assigned for this new created job                                             |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connector``            | The validation status for the inputs of connector forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+
| ``framework``            | The validation status for the inputs of framework forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+

* Response Example:

::

  {
    "id":1,
    "connector":{
      "status":"FINE",
      "messages":{

      }
    },
    "framework":{
      "status":"FINE",
      "messages":{

      }
    }
  }

/v1/job/[jid] - [PUT] - Update Job
---------------------------------------------------------

Update an existing job object with id [jid]. To make the procedure of filling inputs easier, the general practice
is get the existing job object first and then change some of the inputs.

* Method: ``PUT``
* Format: ``JSON``
* Fields of Request:

The same as Create Job.

* Fields of Response:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``connector``            | The validation status for the inputs of connector forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+
| ``framework``            | The validation status for the inputs of framework forms, provided by the request     |
+--------------------------+--------------------------------------------------------------------------------------+

* Response Example:

::

  {
    "connector":{
      "status":"FINE",
      "messages":{

      }
    },
    "framework":{
      "status":"FINE",
      "messages":{

      }
    }
  }

/v1/job/[jid] - [DELETE] - Delete Job
---------------------------------------------------------

Delete a job with id ``jid``.

* Method: ``DELETE``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/job/[jid]/enable - [PUT] - Enable Job
---------------------------------------------------------

Enable a job with id ``jid``.

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/job/[jid]/disable - [PUT] - Disable Job
---------------------------------------------------------

Disable a job with id ``jid``.

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/submission/history/[jid] - [GET] - Get Job Submission History
----------------------------------------------------------------------

Retrieve all job submission history of a given job, such as the status, counters and links for those submissions.

Provide the id of the job in the url [jid] part. If you provide ``all`` in the [jid] part in the url, you will get all job submission history of all jobs.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Fields of Response:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``progress``             | The progress of the running Sqoop job                                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``job``                  | The id of the Sqoop job                                                              |
+--------------------------+--------------------------------------------------------------------------------------+
| ``creation-date``        | The submission timestamp                                                             |
+--------------------------+--------------------------------------------------------------------------------------+
| ``last-update-date``     | The timestamp of the last status update                                              |
+--------------------------+--------------------------------------------------------------------------------------+
| ``status``               | The status of this job submission                                                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``external-id``          | The job id of Sqoop job running on Hadoop                                            |
+--------------------------+--------------------------------------------------------------------------------------+
| ``external-link``        | The link to track the job status on Hadoop                                           |
+--------------------------+--------------------------------------------------------------------------------------+

* Response Example:

::

  {
    "all":[
      {
        "progress":-1.0,
        "last-update-date":1375913666476,
        "external-id":"job_201307221513_0009",
        "status":"SUCCEEDED",
        "job":1,
        "creation-date":1375913630576,
        "external-link":"http:\/\/localhost:50030\/jobdetails.jsp?jobid=job_201307221513_0009",
        "counters":{
          "org.apache.hadoop.mapreduce.JobCounter":{
            "SLOTS_MILLIS_MAPS":59135,
            "FALLOW_SLOTS_MILLIS_REDUCES":0,
            "FALLOW_SLOTS_MILLIS_MAPS":0,
            "TOTAL_LAUNCHED_MAPS":2,
            "SLOTS_MILLIS_REDUCES":0
          },
          "org.apache.hadoop.mapreduce.TaskCounter":{
            "MAP_INPUT_RECORDS":0,
            "PHYSICAL_MEMORY_BYTES":231583744,
            "SPILLED_RECORDS":0,
            "COMMITTED_HEAP_BYTES":112721920,
            "CPU_MILLISECONDS":20940,
            "VIRTUAL_MEMORY_BYTES":1955266560,
            "SPLIT_RAW_BYTES":223,
            "MAP_OUTPUT_RECORDS":5
          },
          "org.apache.hadoop.mapreduce.FileSystemCounter":{
            "FILE_WRITE_OPS":0,
            "FILE_READ_OPS":0,
            "FILE_LARGE_READ_OPS":0,
            "FILE_BYTES_READ":0,
            "HDFS_BYTES_READ":223,
            "FILE_BYTES_WRITTEN":386286,
            "HDFS_LARGE_READ_OPS":0,
            "HDFS_WRITE_OPS":2,
            "HDFS_READ_OPS":3,
            "HDFS_BYTES_WRITTEN":72
          },
          "org.apache.sqoop.submission.counter.SqoopCounters":{
            "ROWS_READ":5
          }
        }
      },
      {
        "progress":-1.0,
        "last-update-date":1375913554412,
        "external-id":"job_201307221513_0008",
        "status":"SUCCEEDED",
        "job":1,
        "creation-date":1375913501078,
        "external-link":"http:\/\/localhost:50030\/jobdetails.jsp?jobid=job_201307221513_0008",
        "counters":{
          "org.apache.hadoop.mapreduce.JobCounter":{
            "SLOTS_MILLIS_MAPS":54905,
            "FALLOW_SLOTS_MILLIS_REDUCES":0,
            "FALLOW_SLOTS_MILLIS_MAPS":0,
            "TOTAL_LAUNCHED_MAPS":2,
            "SLOTS_MILLIS_REDUCES":0
          },
          "org.apache.hadoop.mapreduce.TaskCounter":{
            "MAP_INPUT_RECORDS":0,
            "PHYSICAL_MEMORY_BYTES":218865664,
            "SPILLED_RECORDS":0,
            "COMMITTED_HEAP_BYTES":112918528,
            "CPU_MILLISECONDS":2550,
            "VIRTUAL_MEMORY_BYTES":1955266560,
            "SPLIT_RAW_BYTES":223,
            "MAP_OUTPUT_RECORDS":5
          },
          "org.apache.hadoop.mapreduce.FileSystemCounter":{
            "FILE_WRITE_OPS":0,
            "FILE_READ_OPS":0,
            "FILE_LARGE_READ_OPS":0,
            "FILE_BYTES_READ":0,
            "HDFS_BYTES_READ":223,
            "FILE_BYTES_WRITTEN":387362,
            "HDFS_LARGE_READ_OPS":0,
            "HDFS_WRITE_OPS":2,
            "HDFS_READ_OPS":2,
            "HDFS_BYTES_WRITTEN":72
          },
          "org.apache.sqoop.submission.counter.SqoopCounters":{
            "ROWS_READ":5
          }
        }
      }
    ]
  }

/v1/submission/action/[jid] - [GET]- Get The Latest Submission Of A Given Job
---------------------------------------------------------------------------------

Retrieve the latest submission of a given job, such as the status, counters and links for those submissions.

This function is similar to ``/v1/submission/history/[jid]`` except that it always return one submission object.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Fields of Response:

The same as ``/v1/submission/history/[jid]``

* Response Example:

The same as ``/v1/submission/history/[jid]``

/v1/submission/action/[jid] - [POST]- Submit Job
---------------------------------------------------------------------------------

Submit a job with id ``[jid]`` to make it run.

* Method: ``POST``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/submission/action/[jid] - [DELETE]- Stop Job
---------------------------------------------------------------------------------

Stop a job with id ``[jid]``.

* Method: ``DELETE``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``
