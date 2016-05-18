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

This document will explain how you can use Sqoop REST API to build applications interacting with Sqoop server.
The REST API covers all aspects of managing Sqoop jobs and allows you to build an app in any programming language using HTTP over JSON.

.. contents:: Table of Contents

Initialization
=========================

Before continuing further, make sure that the Sqoop server is running.

The Sqoop 2 server exposes its REST API via Jetty. By default the server is accessible over HTTP but it can be configured to use HTTPS, please see: :ref:`apitlsssl` for more information. The endpoints are registered under the ``/sqoop`` path and the port is configurable (the default is 12000). For example, if the host running the Sqoop 2 server is ``example.com`` and we are using the default port, we can reach the version endpoint by sending a GET request to:

 ::

    http://example.com:12000/sqoop/v1/version


Certain requests might need to contain some additional query parameters and post data. These parameters could be given via
the HTTP headers, request body or both. All the content in the HTTP body is in ``JSON`` format.

Understand Connector, Driver, Link and Job
===========================================================

To create and run a Sqoop Job, we need to provide config values for connecting to a data source and then processing the data in that data source. Processing might be either reading from or writing to the data source. Thus we have configurable entities such as the ``From`` and ``To`` parts of the connectors, the driver that each expose configs and one or more inputs within them.

For instance a connector that represents a relational data source such as MySQL will expose config classes for connecting to the database. Some of the relevant inputs are the connection string, driver class, the username and the password to connect to the database. These configs remain the same to read data from any of the tables within that database. Hence they are grouped under ``LinkConfiguration``.

Each connector can support Reading from a data source and/or writing/to a data source it represents. Reading from and writing to a data source are represented by From and To respectively. Specific configurations are required to peform the job of reading from or writing to the data source. These are grouped in the ``FromJobConfiguration`` and ``ToJobConfiguration`` objects of the connector.

For instance, a connector that represents a relational data source such as MySQL will expose the table name to read from or the SQL query to use while reading data as a FromJobConfiguration. Similarly a connector that represents a data source such as HDFS, will expose the output directory to write to as a ToJobConfiguration.


Objects
==============

This section covers all the objects that might exist in an API request and/or API response.

Configs and Inputs
------------------

Before creating any link for a connector or a job with associated ``From`` and ``To`` links, the first thing to do is getting familiar with all the configurations that the connector exposes.

Each config consists of the following information

+------------------+---------------------------------------------------------+
|   Field          | Description                                             |
+==================+=========================================================+
| ``id``           | The id of this config                                   |
+------------------+---------------------------------------------------------+
| ``inputs``       | A array of inputs of this config                        |
+------------------+---------------------------------------------------------+
| ``name``         | The unique name of this config per connector            |
+------------------+---------------------------------------------------------+
| ``type``         | The type of this config (LINK/ JOB)                     |
+------------------+---------------------------------------------------------+

A typical config object is showing below:

::

   {
    id:7,
    inputs:[
      {
         id: 25,
         name: "throttlingConfig.numExtractors",
         type: "INTEGER",
         sensitive: false
      },
      {
         id: 26,
         name: "throttlingConfig.numLoaders",
         type: "INTEGER",
         sensitive: false
       }
    ],
    name: "throttlingConfig",
    type: "JOB"
  }

Each input object in a config is structured below:

+------------------+---------------------------------------------------------+
|   Field          | Description                                             |
+==================+=========================================================+
| ``id``           | The id of this input                                    |
+------------------+---------------------------------------------------------+
| ``name``         | The unique name of this input per config                |
+------------------+---------------------------------------------------------+
| ``type``         | The data type of this input field                       |
+------------------+---------------------------------------------------------+
| ``size``         | The length of this input field                          |
+------------------+---------------------------------------------------------+
| ``sensitive``    | Whether this input contain sensitive information        |
+------------------+---------------------------------------------------------+


To send a filled config in the request, you should always use config id and input id to map the values to their correspondig names.
For example, the following request contains an input value ``com.mysql.jdbc.Driver`` with input id ``7`` inside a config with id ``4`` that belongs to a link with name ``linkName``

::

      link: {
            id : 3,
            name: "linkName",
            enabled: true,
            link-config-values: [{
                id: 4,
                inputs: [{
                    id: 7,
                    name: "linkConfig.jdbcDriver",
                    value: "com.mysql.jdbc.Driver",
                    type: "STRING",
                    size: 128,
                    sensitive: false
                }, {
                    id: 8,
                    name: "linkConfig.connectionString",
                    value: "jdbc%3Amysql%3A%2F%2Fmysql.ent.cloudera.com%2Fsqoop",
                    type: "STRING",
                    size: 128,
                    sensitive: false
                },
                ...
             }
           }

Exception Response
------------------

Each operation on Sqoop server might return an exception in the Http response. Remember to take this into account.The exception code and message could be found in both the header and body of the response.

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

Config and Input Validation Status Response
--------------------------------------------

The config and the inputs associated with the connectors also provide custom validation rules for the values given to these input fields. Sqoop applies these custom validators and its corresponding valdation logic when config values for the LINK and JOB are posted.


An example of a OK status with the persisted ID:
::

 {
    "id": 3,
    "validation-result": [
        {}
    ]
 }

An example of ERROR status:
::

   {
     "validation-result": [
       {
        "linkConfig": [
          {
            "message": "Invalid URI. URI must either be null or a valid URI. Here are a few valid example URIs: hdfs://example.com:8020/, hdfs://example.com/, file:///, file:///tmp, file://localhost/tmp",
            "status": "ERROR"
          }
        ]
      }
     ]
   }

Job Submission Status Response
------------------------------

After starting a job, you could look up the running status of it. There could be 7 possible status:

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
=================

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

REST APIs
==========

The section elaborates all the rest apis that are supported by the Sqoop server.

For all Sqoop requests, the following request parameters will be added automatically. However, this user name is only in simple mode. In Kerberos mode, this user name will be ignored by Sqoop server and user name in UGI which is authenticated by Kerberos server will be used instead.

+---------------------------+---------------------------------------------------------+
|   Parameter               | Description                                             |
+===========================+=========================================================+
| ``user.name``             | The name of the user who makes the requests             |
+---------------------------+---------------------------------------------------------+


/version - [GET] - Get Sqoop Version
-------------------------------------

Get all the version metadata of Sqoop software in the server side.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Fields of Response:

+--------------------+---------------------------------------------------------+
|   Field            | Description                                             |
+====================+=========================================================+
| ``source-revision``| The revision number of Sqoop source code                |
+--------------------+---------------------------------------------------------+
| ``api-versions``   | The version of network protocol                         |
+--------------------+---------------------------------------------------------+
| ``build-date``     | The Sqoop release date                                  |
+--------------------+---------------------------------------------------------+
| ``user``           | The user who made the release                           |
+--------------------+---------------------------------------------------------+
| ``source-url``     | The url of the source code trunk                        |
+--------------------+---------------------------------------------------------+
| ``build-version``  | The version of Sqoop in the server side                 |
+--------------------+---------------------------------------------------------+


* Response Example:

::

   {
    source-url: "git://vbasavaraj.local/Users/vbasavaraj/Projects/SqoopRefactoring/sqoop2/common",
    source-revision: "418c5f637c3f09b94ea7fc3b0a4610831373a25f",
    build-version: "2.0.0-SNAPSHOT",
    api-versions: [
       "v1"
     ],
    user: "vbasavaraj",
    build-date: "Mon Nov 3 08:18:21 PST 2014"
   }

/v1/connectors - [GET]  Get all Connectors
-------------------------------------------

Get all the connectors registered in Sqoop

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Response Example

::

  {
    connectors: [{
        id: 1,
        link-config: [],
        job-config: {},
        name: "hdfs-connector",
        class: "org.apache.sqoop.connector.hdfs.HdfsConnector",
        all-config-resources: {},
        version: "2.0.0-SNAPSHOT"
    }, {
        id: 2,
        link-config: [],
        job-config: {},
        name: "generic-jdbc-connector",
        class: "org.apache.sqoop.connector.jdbc.GenericJdbcConnector",
        all-config - resources: {},
        version: "2.0.0-SNAPSHOT"
    }]
  }

/v1/connector/[cname] - [GET] - Get Connector
---------------------------------------------------------------------

Provide the unique name of the connector in the url ``[cname]`` part.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Fields of Response:

+--------------------------+----------------------------------------------------------------------------------------+
|   Field                  | Description                                                                            |
+==========================+========================================================================================+
| ``name``                 | The name for the connector ( registered as a configurable )                            |
+--------------------------+----------------------------------------------------------------------------------------+
| ``job-config``           | Connector job config and inputs for both FROM and TO                                   |
+--------------------------+----------------------------------------------------------------------------------------+
| ``link-config``          | Connector link config and inputs                                                       |
+--------------------------+----------------------------------------------------------------------------------------+
| ``all-config-resources`` | All config inputs labels and description for the given connector                       |
+--------------------------+----------------------------------------------------------------------------------------+
| ``version``              | The build version required for config and input data upgrades                          |
+--------------------------+----------------------------------------------------------------------------------------+

* Response Example:

::

   {
    connector: {
        id: 1,
        name: "connectorName",
        job-config: {
            TO: [{
                id: 3,
                inputs: [{
                    id: 3,
                    values: "TEXT_FILE,SEQUENCE_FILE",
                    name: "toJobConfig.outputFormat",
                    type: "ENUM",
                    sensitive: false
                }, {
                    id: 4,
                    values: "NONE,DEFAULT,DEFLATE,GZIP,BZIP2,LZO,LZ4,SNAPPY,CUSTOM",
                    name: "toJobConfig.compression",
                    type: "ENUM",
                    sensitive: false
                }, {
                    id: 5,
                    name: "toJobConfig.customCompression",
                    type: "STRING",
                    size: 255,
                    sensitive: false
                }, {
                    id: 6,
                    name: "toJobConfig.outputDirectory",
                    type: "STRING",
                    size: 255,
                    sensitive: false
                }],
                name: "toJobConfig",
                type: "JOB"
            }],
            FROM: [{
                id: 2,
                inputs: [{
                    id: 2,
                    name: "fromJobConfig.inputDirectory",
                    type: "STRING",
                    size: 255,
                    sensitive: false
                }],
                name: "fromJobConfig",
                type: "JOB"
            }]
        },
        link-config: [{
            id: 1,
            inputs: [{
                id: 1,
                name: "linkConfig.uri",
                type: "STRING",
                size: 255,
                sensitive: false
            }],
            name: "linkConfig",
            type: "LINK"
        }],
        name: "hdfs-connector",
        class: "org.apache.sqoop.connector.hdfs.HdfsConnector",
        all-config-resources: {
            fromJobConfig.label: "From Job configuration",
                toJobConfig.ignored.label: "Ignored",
                fromJobConfig.help: "Specifies information required to get data from Hadoop ecosystem",
                toJobConfig.ignored.help: "This value is ignored",
                toJobConfig.label: "ToJob configuration",
                toJobConfig.storageType.label: "Storage type",
                fromJobConfig.inputDirectory.label: "Input directory",
                toJobConfig.outputFormat.label: "Output format",
                toJobConfig.outputDirectory.label: "Output directory",
                toJobConfig.outputDirectory.help: "Output directory for final data",
                toJobConfig.compression.help: "Compression that should be used for the data",
                toJobConfig.outputFormat.help: "Format in which data should be serialized",
                toJobConfig.customCompression.label: "Custom compression format",
                toJobConfig.compression.label: "Compression format",
                linkConfig.label: "Link configuration",
                toJobConfig.customCompression.help: "Full class name of the custom compression",
                toJobConfig.storageType.help: "Target on Hadoop ecosystem where to store data",
                linkConfig.help: "Here you supply information necessary to connect to HDFS",
                linkConfig.uri.help: "HDFS URI used to connect to HDFS",
                linkConfig.uri.label: "HDFS URI",
                fromJobConfig.inputDirectory.help: "Directory that should be exported",
                toJobConfig.help: "You must supply the information requested in order to get information where you want to store your data."
        },
        version: "2.0.0-SNAPSHOT"
     }
   }


/v1/driver - [GET]- Get Sqoop Driver
-----------------------------------------------

Driver exposes configurations required for the job execution.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Fields of Response:

+--------------------------+----------------------------------------------------------------------------------------------------+
|   Field                  | Description                                                                                        |
+==========================+====================================================================================================+
| ``id``                   | The id for the driver ( registered as a configurable )                                             |
+--------------------------+----------------------------------------------------------------------------------------------------+
| ``job-config``           | Driver job config and inputs                                                                       |
+--------------------------+----------------------------------------------------------------------------------------------------+
| ``version``              | The build version of the driver                                                                    |
+--------------------------+----------------------------------------------------------------------------------------------------+
| ``all-config-resources`` | Driver exposed config and input labels and description                                             |
+--------------------------+----------------------------------------------------------------------------------------------------+

* Response Example:

::

 {
    id: 3,
    job-config: [{
        id: 7,
        inputs: [{
            id: 25,
            name: "throttlingConfig.numExtractors",
            type: "INTEGER",
            sensitive: false
        }, {
            id: 26,
            name: "throttlingConfig.numLoaders",
            type: "INTEGER",
            sensitive: false
        }],
        name: "throttlingConfig",
        type: "JOB"
    }],
    all-config-resources: {
        throttlingConfig.numExtractors.label: "Extractors",
            throttlingConfig.numLoaders.help: "Number of loaders that Sqoop will use",
            throttlingConfig.numLoaders.label: "Loaders",
            throttlingConfig.label: "Throttling resources",
            throttlingConfig.numExtractors.help: "Number of extractors that Sqoop will use",
            throttlingConfig.help: "Set throttling boundaries to not overload your systems"
    },
    version: "1"
 }

/v1/links/ - [GET]  Get all links
-------------------------------------------

Get all the links created in Sqoop

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Response Example

::

  {
    links: [
      {
        id: 1,
        enabled: true,
        update-user: "root",
        link-config-values: [],
        name: "First Link",
        creation-date: 1415309361756,
        connector-name: "connectorName1",
        update-date: 1415309361756,
        creation-user: "root"
      },
      {
        id: 2,
        enabled: true,
        update-user: "root",
        link-config-values: [],
        name: "Second Link",
        creation-date: 1415309390807,
        connector-name: "connectorName2",
        update-date: 1415309390807,
        creation-user: "root"
      }
    ]
  }


/v1/links?cname=[cname] - [GET]  Get all links by Connector
------------------------------------------------------------
Get all the links for a given connector identified by ``[cname]`` part.


/v1/link/[lname]  - [GET] - Get Link
-------------------------------------------------------------------------------

Provide the unique name of the link in the url ``[lname]`` part.

Get all the details of the link including the name, type and the corresponding config input values for the link


* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Response Example:

::

 {
    link: {
        id: 1,
        enabled: true,
        link-config-values: [{
            id: 1,
            inputs: [{
                id: 1,
                name: "linkConfig.uri",
                value: "hdfs%3A%2F%2Fnamenode%3A8090",
                type: "STRING",
                size: 255,
                sensitive: false
            }],
            name: "linkConfig",
            type: "LINK"
        }],
        update-user: "root",
        name: "First Link",
        creation-date: 1415287846371,
        connector-name: "connectorName",
        update-date: 1415287846371,
        creation-user: "root"
    }
 }

/v1/link - [POST] - Create Link
---------------------------------------------------------

Create a new link object. Provide values to the link config inputs for the ones that are required.

* Method: ``POST``
* Format: ``JSON``
* Fields of Request:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``link``                 | The root of the post data in JSON                                                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``id``                   | The id of the link can be left blank in the post data                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``enabled``              | Whether to enable this link (true/false)                                             |
+--------------------------+--------------------------------------------------------------------------------------+
| ``update-date``          | The last updated time of this link                                                   |
+--------------------------+--------------------------------------------------------------------------------------+
| ``creation-date``        | The creation time of this link                                                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``update-user``          | The user who updated this link                                                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``creation-user``        | The user who created this link                                                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``name``                 | The name of this link                                                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``link-config-values``   | Config input values for link config for the corresponding connector                  |
+--------------------------+--------------------------------------------------------------------------------------+
| ``connector-id``         | The id of the connector used for this link                                           |
+--------------------------+--------------------------------------------------------------------------------------+

* Request Example:

::

  {
    link: {
        id: -1,
        enabled: true,
        link-config-values: [{
            id: 1,
            inputs: [{
                id: 1,
                name: "linkConfig.uri",
                value: "hdfs%3A%2F%2Fvbsqoop-1.ent.cloudera.com%3A8020%2Fuser%2Froot%2Fjob1",
                type: "STRING",
                size: 255,
                sensitive: false
            }],
            name: "testInput",
            type: "LINK"
        }],
        update-user: "root",
        name: "testLink",
        creation-date: 1415202223048,
        connector-name: "connectorName",
        update-date: 1415202223048,
        creation-user: "root"
    }
  }

* Fields of Response:

+---------------------------+--------------------------------------------------------------------------------------+
|   Field                   | Description                                                                          |
+===========================+======================================================================================+
| ``name``                  | The name assigned for this new created link                                          |
+---------------------------+--------------------------------------------------------------------------------------+
| ``validation-result``     | The validation status for the  link config inputs given in the post data             |
+---------------------------+--------------------------------------------------------------------------------------+

* ERROR Response Example:

::

   {
     "validation-result": [
         {
             "linkConfig": [
                 {
                     "message": "Invalid URI. URI must either be null or a valid URI. Here are a few valid example URIs: hdfs://example.com:8020/, hdfs://example.com/, file:///, file:///tmp, file://localhost/tmp",
                     "status": "ERROR"
                 }
             ]
         }
     ]
   }


/v1/link/[lname] - [PUT] - Update Link
---------------------------------------------------------

Update an existing link object with name [lname]. To make the procedure of filling inputs easier, the general practice
is get the link first and then change some of the values for the inputs.

* Method: ``PUT``
* Format: ``JSON``

* OK Response Example:

::

  {
    "validation-result": [
        {}
    ]
  }

/v1/link/[lname]  - [DELETE] - Delete Link
-----------------------------------------------------------------

Delete a link with name [lname]

* Method: ``DELETE``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/link/[lname]/enable  - [PUT] - Enable Link
--------------------------------------------------------------------------------

Enable a link with name ``lname``

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/link/[lname]/disable - [PUT] - Disable Link
---------------------------------------------------------

Disable a link with name ``lname``

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/jobs/ - [GET]  Get all jobs
-------------------------------------------

Get all the jobs created in Sqoop

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Response Example:

::

  {
     jobs: [{
        driver-config-values: [],
            enabled: true,
            from-connector-name: "fromConnectorName",
            update-user: "root",
            to-config-values: [],
            to-connector-name: "toConnectorName",
            creation-date: 1415310157618,
            update-date: 1415310157618,
            creation-user: "root",
            id: 1,
            to-link-name: "toLinkName",
            from-config-values: [],
            name: "First Job",
            from-link-name: "fromLinkName"
       },{
        driver-config-values: [],
            enabled: true,
            from-connector-name: "fromConnectorName",
            update-user: "root",
            to-config-values: [],
            to-connector-name: "toConnectorName",
            creation-date: 1415310650600,
            update-date: 1415310650600,
            creation-user: "root",
            id: 2,
            to-link-name: "toLinkName",
            from-config-values: [],
            name: "Second Job",
            from-link-name: "fromLinkName"
       }]
  }

/v1/jobs?cname=[cname] - [GET]  Get all jobs by connector
------------------------------------------------------------
Get all the jobs for a given connector identified by ``[cname]`` part.


/v1/job/[jname] - [GET] - Get Job
-----------------------------------------------------

Provide the name of the job in the url [jname] part.

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``

* Response Example:

::

  {
    job: {
        driver-config-values: [{
                id: 7,
                inputs: [{
                    id: 25,
                    name: "throttlingConfig.numExtractors",
                    value: "3",
                    type: "INTEGER",
                    sensitive: false
                }, {
                    id: 26,
                    name: "throttlingConfig.numLoaders",
                    value: "3",
                    type: "INTEGER",
                    sensitive: false
                }],
                name: "throttlingConfig",
                type: "JOB"
            }],
            enabled: true,
            from-connector-name: "fromConnectorName",
            update-user: "root",
            to-config-values: [{
                id: 6,
                inputs: [{
                    id: 19,
                    name: "toJobConfig.schemaName",
                    type: "STRING",
                    size: 50,
                    sensitive: false
                }, {
                    id: 20,
                    name: "toJobConfig.tableName",
                    value: "text",
                    type: "STRING",
                    size: 2000,
                    sensitive: false
                }, {
                    id: 21,
                    name: "toJobConfig.sql",
                    type: "STRING",
                    size: 50,
                    sensitive: false
                }, {
                    id: 22,
                    name: "toJobConfig.columns",
                    type: "STRING",
                    size: 50,
                    sensitive: false
                }, {
                    id: 23,
                    name: "toJobConfig.stageTableName",
                    type: "STRING",
                    size: 2000,
                    sensitive: false
                }, {
                    id: 24,
                    name: "toJobConfig.shouldClearStageTable",
                    type: "BOOLEAN",
                    sensitive: false
                }],
                name: "toJobConfig",
                type: "JOB"
            }],
            to-connector-name: "toConnectorName",
            creation-date: 1415310157618,
            update-date: 1415310157618,
            creation-user: "root",
            id: 1,
            to-link-name: "toLinkName",
            from-config-values: [{
                id: 2,
                inputs: [{
                    id: 2,
                    name: "fromJobConfig.inputDirectory",
                    value: "hdfs%3A%2F%2Fvbsqoop-1.ent.cloudera.com%3A8020%2Fuser%2Froot%2Fjob1",
                    type: "STRING",
                    size: 255,
                    sensitive: false
                }],
                name: "fromJobConfig",
                type: "JOB"
            }],
            name: "First Job",
            from-link-name: "fromLinkName"
    }
 }


/v1/job - [POST] - Create Job
---------------------------------------------------------

Create a new job object with the corresponding config values.

* Method: ``POST``
* Format: ``JSON``

* Fields of Request:


+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``job``                  | The root of the post data in JSON                                                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``from-link-name``       | The name of the from link for the job                                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``to-link-name``         | The name of the to link for the job                                                  |
+--------------------------+--------------------------------------------------------------------------------------+
| ``id``                   | The id of the link can be left blank in the post data                                |
+--------------------------+--------------------------------------------------------------------------------------+
| ``enabled``              | Whether to enable this job (true/false)                                              |
+--------------------------+--------------------------------------------------------------------------------------+
| ``update-date``          | The last updated time of this job                                                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``creation-date``        | The creation time of this job                                                        |
+--------------------------+--------------------------------------------------------------------------------------+
| ``update-user``          | The user who updated this job                                                        |
+--------------------------+--------------------------------------------------------------------------------------+
| ``creation-user``        | The uset who creates this job                                                        |
+--------------------------+--------------------------------------------------------------------------------------+
| ``name``                 | The name of this job                                                                 |
+--------------------------+--------------------------------------------------------------------------------------+
| ``from-config-values``   | Config input values for FROM part of the job                                         |
+--------------------------+--------------------------------------------------------------------------------------+
| ``to-config-values``     | Config input values for TO part of the job                                           |
+--------------------------+--------------------------------------------------------------------------------------+
| ``driver-config-values`` | Config input values for driver                                                       |
+--------------------------+--------------------------------------------------------------------------------------+
| ``from-connector-name``  | The name of the from connector for the job                                           |
+--------------------------+--------------------------------------------------------------------------------------+
| ``to-connector-name``    | The name of the to connector for the job                                             |
+--------------------------+--------------------------------------------------------------------------------------+

* Request Example:

::

 {
   job: {
     driver-config-values: [
       {
         id: 7,
         inputs: [
           {
             id: 25,
             name: "throttlingConfig.numExtractors",
             value: "3",
             type: "INTEGER",
             sensitive: false
           },
           {
             id: 26,
             name: "throttlingConfig.numLoaders",
             value: "3",
             type: "INTEGER",
             sensitive: false
           }
         ],
         name: "throttlingConfig",
         type: "JOB"
       }
     ],
     enabled: true,
     from-connector-name: "fromConnectorName",
     update-user: "root",
     to-config-values: [
       {
         id: 6,
         inputs: [
           {
             id: 19,
             name: "toJobConfig.schemaName",
             type: "STRING",
             size: 50,
             sensitive: false
           },
           {
             id: 20,
             name: "toJobConfig.tableName",
             value: "text",
             type: "STRING",
             size: 2000,
             sensitive: false
           },
           {
             id: 21,
             name: "toJobConfig.sql",
             type: "STRING",
             size: 50,
             sensitive: false
           },
           {
             id: 22,
             name: "toJobConfig.columns",
             type: "STRING",
             size: 50,
             sensitive: false
           },
           {
             id: 23,
             name: "toJobConfig.stageTableName",
             type: "STRING",
             size: 2000,
             sensitive: false
           },
           {
             id: 24,
             name: "toJobConfig.shouldClearStageTable",
             type: "BOOLEAN",
             sensitive: false
           }
         ],
         name: "toJobConfig",
         type: "JOB"
       }
     ],
     to-connector-name: "toConnectorName",
     creation-date: 1415310157618,
     update-date: 1415310157618,
     creation-user: "root",
     id: -1,
     to-link-name: "toLinkName",
     from-config-values: [
       {
         id: 2,
         inputs: [
           {
             id: 2,
             name: "fromJobConfig.inputDirectory",
             value: "hdfs%3A%2F%2Fvbsqoop-1.ent.cloudera.com%3A8020%2Fuser%2Froot%2Fjob1",
             type: "STRING",
             size: 255,
             sensitive: false
           }
         ],
         name: "fromJobConfig",
         type: "JOB"
       }
     ],
     name: "Test Job",
     from-link-name: "fromLinkName"
    }
  }

* Fields of Response:

+---------------------------+--------------------------------------------------------------------------------------+
|   Field                   | Description                                                                          |
+===========================+======================================================================================+
| ``name``                  | The name assigned for this new created job                                           |
+--------------------------+---------------------------------------------------------------------------------------+
| ``validation-result``     | The validation status for the job config and driver config inputs in the post data   |
+---------------------------+--------------------------------------------------------------------------------------+


* ERROR Response Example:

::

   {
     "validation-result": [
         {
             "linkConfig": [
                 {
                     "message": "Invalid URI. URI must either be null or a valid URI. Here are a few valid example URIs: hdfs://example.com:8020/, hdfs://example.com/, file:///, file:///tmp, file://localhost/tmp",
                     "status": "ERROR"
                 }
             ]
         }
     ]
   }


/v1/job/[jname] - [PUT] - Update Job
---------------------------------------------------------

Update an existing job object with name [jname]. To make the procedure of filling inputs easier, the general practice
is get the existing job object first and then change some of the inputs.

* Method: ``PUT``
* Format: ``JSON``

The same as Create Job.

* OK Response Example:

::

  {
    "validation-result": [
        {}
    ]
  }


/v1/job/[jname] - [DELETE] - Delete Job
---------------------------------------------------------

Delete a job with name ``jname``.

* Method: ``DELETE``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/job/[jname]/enable - [PUT] - Enable Job
---------------------------------------------------------

Enable a job with name ``jname``.

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/job/[jname]/disable - [PUT] - Disable Job
---------------------------------------------------------

Disable a job with name ``jname``.

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``


/v1/job/[jname]/start - [PUT]- Start Job
---------------------------------------------------------------------------------

Start a job with name ``[jname]`` to trigger the job execution

* Method: ``POST``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``Submission Record``

* BOOTING Response Example

::

  {
    "submission": {
      "progress": -1,
      "last-update-date": 1415312531188,
      "external-id": "job_1412137947693_0004",
      "status": "BOOTING",
      "job": 2,
      "job-name": "jobName",
      "creation-date": 1415312531188,
      "to-schema": {
        "created": 1415312531426,
        "name": "HDFS file",
        "columns": []
      },
      "external-link": "http://vbsqoop-1.ent.cloudera.com:8088/proxy/application_1412137947693_0004/",
      "from-schema": {
        "created": 1415312531342,
        "name": "text",
        "columns": [
          {
            "name": "id",
            "nullable": true,
            "unsigned": null,
            "type": "FIXED_POINT",
            "size": null
          },
          {
            "name": "txt",
            "nullable": true,
            "type": "TEXT",
            "size": null
          }
        ]
      }
    }
  }

* SUCCEEDED Response Example

::

   {
     submission: {
       progress: -1,
       last-update-date: 1415312809485,
       external-id: "job_1412137947693_0004",
       status: "SUCCEEDED",
       job: 2,
       job-name: "jobName",
       creation-date: 1415312531188,
       external-link: "http://vbsqoop-1.ent.cloudera.com:8088/proxy/application_1412137947693_0004/",
       counters: {
         org.apache.hadoop.mapreduce.JobCounter: {
           SLOTS_MILLIS_MAPS: 373553,
           MB_MILLIS_MAPS: 382518272,
           TOTAL_LAUNCHED_MAPS: 10,
           MILLIS_MAPS: 373553,
           VCORES_MILLIS_MAPS: 373553,
           OTHER_LOCAL_MAPS: 10
         },
         org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter: {
           BYTES_WRITTEN: 0
         },
         org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter: {
           BYTES_READ: 0
         },
         org.apache.hadoop.mapreduce.TaskCounter: {
           MAP_INPUT_RECORDS: 0,
           MERGED_MAP_OUTPUTS: 0,
           PHYSICAL_MEMORY_BYTES: 4065599488,
           SPILLED_RECORDS: 0,
           COMMITTED_HEAP_BYTES: 3439853568,
           CPU_MILLISECONDS: 236900,
           FAILED_SHUFFLE: 0,
           VIRTUAL_MEMORY_BYTES: 15231422464,
           SPLIT_RAW_BYTES: 1187,
           MAP_OUTPUT_RECORDS: 1000000,
           GC_TIME_MILLIS: 7282
         },
         org.apache.hadoop.mapreduce.FileSystemCounter: {
           FILE_WRITE_OPS: 0,
           FILE_READ_OPS: 0,
           FILE_LARGE_READ_OPS: 0,
           FILE_BYTES_READ: 0,
           HDFS_BYTES_READ: 1187,
           FILE_BYTES_WRITTEN: 1191230,
           HDFS_LARGE_READ_OPS: 0,
           HDFS_WRITE_OPS: 10,
           HDFS_READ_OPS: 10,
           HDFS_BYTES_WRITTEN: 276389736
         },
         org.apache.sqoop.submission.counter.SqoopCounters: {
           ROWS_READ: 1000000
         }
       }
     }
   }


* ERROR Response Example

::

  {
    "submission": {
      "progress": -1,
      "last-update-date": 1415312390570,
      "status": "FAILURE_ON_SUBMIT",
      "error-summary": "org.apache.sqoop.common.SqoopException: GENERIC_HDFS_CONNECTOR_0000:Error occurs during partitioner run",
      "job": 1,
      "job-name": "jobName",
      "creation-date": 1415312390570,
      "to-schema": {
        "created": 1415312390797,
        "name": "text",
        "columns": [
          {
            "name": "id",
            "nullable": true,
            "unsigned": null,
            "type": "FIXED_POINT",
            "size": null
          },
          {
            "name": "txt",
            "nullable": true,
            "type": "TEXT",
            "size": null
          }
        ]
      },
      "from-schema": {
        "created": 1415312390778,
        "name": "HDFS file",
        "columns": [
        ]
      },
      "error-details": "org.apache.sqoop.common.SqoopException: GENERIC_HDFS_CONNECTOR_00"
    }
  }

/v1/job/[jname]/stop  - [PUT]- Stop Job
---------------------------------------------------------------------------------

Stop a job with name ``[jname]`` to abort the running job.

* Method: ``PUT``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``Submission Record``

/v1/job/[jname]/status  - [GET]- Get Job Status
---------------------------------------------------------------------------------

Get status of the running job with name ``[jname]``

* Method: ``GET``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``Submission Record``

::

  {
      "submission": {
          "progress": 0.25,
          "last-update-date": 1415312603838,
          "external-id": "job_1412137947693_0004",
          "status": "RUNNING",
          "job": 2,
          "job-name": "jobName",
          "creation-date": 1415312531188,
          "external-link": "http://vbsqoop-1.ent.cloudera.com:8088/proxy/application_1412137947693_0004/"
      }
  }

/v1/submissions? - [GET] - Get all job Submissions
----------------------------------------------------------------------

Get all the submissions for every job started in SQoop

/v1/submissions?jname=[jname] - [GET] - Get Submissions by Job
----------------------------------------------------------------------

Retrieve all job submissions in the past for the given job. Each submission record will have details such as the status, counters and urls for those submissions.

Provide the name of the job in the url [jname] part.

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
| ``job-name``             | The name of the Sqoop job                                                            |
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
    submissions: [
      {
        progress: -1,
        last-update-date: 1415312809485,
        external-id: "job_1412137947693_0004",
        status: "SUCCEEDED",
        job: 2,
        job-name: "jobName",
        creation-date: 1415312531188,
        external-link: "http://vbsqoop-1.ent.cloudera.com:8088/proxy/application_1412137947693_0004/",
        counters: {
          org.apache.hadoop.mapreduce.JobCounter: {
            SLOTS_MILLIS_MAPS: 373553,
            MB_MILLIS_MAPS: 382518272,
            TOTAL_LAUNCHED_MAPS: 10,
            MILLIS_MAPS: 373553,
            VCORES_MILLIS_MAPS: 373553,
            OTHER_LOCAL_MAPS: 10
          },
          org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter: {
            BYTES_WRITTEN: 0
          },
          org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter: {
            BYTES_READ: 0
          },
          org.apache.hadoop.mapreduce.TaskCounter: {
            MAP_INPUT_RECORDS: 0,
            MERGED_MAP_OUTPUTS: 0,
            PHYSICAL_MEMORY_BYTES: 4065599488,
            SPILLED_RECORDS: 0,
            COMMITTED_HEAP_BYTES: 3439853568,
            CPU_MILLISECONDS: 236900,
            FAILED_SHUFFLE: 0,
            VIRTUAL_MEMORY_BYTES: 15231422464,
            SPLIT_RAW_BYTES: 1187,
            MAP_OUTPUT_RECORDS: 1000000,
            GC_TIME_MILLIS: 7282
          },
          org.apache.hadoop.mapreduce.FileSystemCounter: {
            FILE_WRITE_OPS: 0,
            FILE_READ_OPS: 0,
            FILE_LARGE_READ_OPS: 0,
            FILE_BYTES_READ: 0,
            HDFS_BYTES_READ: 1187,
            FILE_BYTES_WRITTEN: 1191230,
            HDFS_LARGE_READ_OPS: 0,
            HDFS_WRITE_OPS: 10,
            HDFS_READ_OPS: 10,
            HDFS_BYTES_WRITTEN: 276389736
          },
          org.apache.sqoop.submission.counter.SqoopCounters: {
            ROWS_READ: 1000000
          }
        }
      },
      {
        progress: -1,
        last-update-date: 1415312390570,
        status: "FAILURE_ON_SUBMIT",
        error-summary: "org.apache.sqoop.common.SqoopException: GENERIC_HDFS_CONNECTOR_0000:Error occurs during partitioner run",
        job: 1,
        job-name: "jobName",
        creation-date: 1415312390570,
        error-details: "org.apache.sqoop.common.SqoopException: GENERIC_HDFS_CONNECTOR_0000:Error occurs during partitioner...."
      }
    ]
  }

/v1/authorization/roles/create - [POST] - Create Role
-----------------------------------------------------

Create a new role object. Provide values to the link config inputs for the ones that are required.

* Method: ``POST``
* Format: ``JSON``
* Fields of Request:

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``role``                 | The root of the post data in JSON                                                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``name``                 | The name of this role                                                                |
+--------------------------+--------------------------------------------------------------------------------------+

* Request Example:

::

  {
    role: {
        name: "testRole",
    }
  }

/v1/authorization/role/[role-name]  - [DELETE] - Delete Role
------------------------------------------------------------

Delete a role with name [role-name]

* Method: ``DELETE``
* Format: ``JSON``
* Request Content: ``None``
* Response Content: ``None``

/v1/authorization/roles?principal_type=[principal-type]&principal_name=[principal-name] - [GET]  Get all Roles by Principal
---------------------------------------------------------------------------------------------------------------------------

Get all the roles or for a given principal identified by ``[principal-type]`` and ``[principal-name]`` part.

/v1/authorization/principals?role_name=[rname] - [GET]  Get all Principals by Role
----------------------------------------------------------------------------------

Get all the principals for a given role identified by ``[rname]`` part.

/v1/authorization/roles/grant - [PUT] - Grant a Role to a Principal
-------------------------------------------------------------------

Grant a role with ``[role-name]`` to a principal with ``[principal-type]`` and ``[principal-name]``.

* Method: ``PUT``
* Format: ``JSON``
* Fields of Request:

The same as Create Role and

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``principals``           | The root of the post data in JSON                                                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``name``                 | The name of this principal                                                           |
+--------------------------+--------------------------------------------------------------------------------------+
| ``type``                 | The type of this principal, ("USER", "GROUP", "ROLE")                                |
+--------------------------+--------------------------------------------------------------------------------------+

* Request Example:

::

  {
    roles: [{
        name: "testRole",
    }],
    principals: [{
        name: "testPrincipalName",
        type: "USER",
    }]
  }

* Response Content: ``None``

/v1/authorization/roles/revoke - [PUT] - Revoke a Role from a Principal
-----------------------------------------------------------------------

Revoke a role with ``[role-name]`` to a principal with ``[principal-type]`` and ``[principal-name]``.

* Method: ``PUT``
* Format: ``JSON``
* Fields of Request:

The same as Grant Role

* Response Content: ``None``

/v1/authorization/privileges/grant - [PUT] - Grant a Privilege to a Principal
-----------------------------------------------------------------------------

Grant a privilege with ``[resource-name]``, ``[resource-type]``, ``[action]`` and ``[with-grant-option]`` to a principal with``[principal-type]`` and ``[principal-name]``.

* Method: ``PUT``
* Format: ``JSON``
* Fields of Request:

The same as Principal and

+--------------------------+--------------------------------------------------------------------------------------+
|   Field                  | Description                                                                          |
+==========================+======================================================================================+
| ``privileges``           | The root of the post data in JSON                                                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``resource-name``        | The resource name of this privilege                                                  |
+--------------------------+--------------------------------------------------------------------------------------+
| ``resource-type``        | The resource type of this privilege, ("CONNECTOR", "LINK", "JOB")                    |
+--------------------------+--------------------------------------------------------------------------------------+
| ``action``               | The action type of this privilege, ("READ", "WRITE", "ALL")                          |
+--------------------------+--------------------------------------------------------------------------------------+
| ``with-grant-option``    | The resource type of this privilege                                                  |
+--------------------------+--------------------------------------------------------------------------------------+

* Request Example:

::

  {
    privileges: [{
        resource-name: "testResourceName",
        resource-type: "LINK",
        action: "READ",
        with-grant-option: false,
    }]
    principals: [{
        name: "testPrincipalName",
        type: "USER",
    }]
  }

* Response Content: ``None``

/v1/authorization/privileges/revoke - [PUT] - Revoke a Privilege to a Principal
-------------------------------------------------------------------------------

Revoke a privilege with ``[resource-name]``, ``[resource-type]``, ``[action]`` and ``[with-grant-option]`` to a principal with``[principal-type]`` and ``[principal-name]``.

* Method: ``PUT``
* Format: ``JSON``
* Fields of Request:

The same as Grant Privilege

* Response Content: ``None``

/v1/authorization/privilieges?principal_type=[principal-type]&principal_name=[principal-name]&resource_type=[resource-type]&resource_name=[resource-name] - [GET]  Get all Roles by Principal (and Resource)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Get all the privileges or for a given principal identified by ``[principal-type]`` and ``[principal-name]`` (and a given resource identified by ``[resource-type]`` and ``[resource-name]``).