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


=============================
Sqoop 2 Connector Development
=============================

This document describes you how to implement connector for Sqoop 2
using the code of built-in connector ( ``GenericJdbcConnector`` ) as example.

.. contents::

What is Connector?
++++++++++++++++++

Connector provides interaction with external databases.
Connector reads data from databases for import,
and write data to databases for export.
Interaction with Hadoop is taken cared by common modules of Sqoop 2 framework.


Connector Implementation
++++++++++++++++++++++++

The ``SqoopConnector`` class defines functionality
which must be provided by Connectors.
Each Connector must extends ``SqoopConnector`` and overrides methods shown below.
::

  public abstract String getVersion();
  public abstract ResourceBundle getBundle(Locale locale);
  public abstract Class getConnectionConfigurationClass();
  public abstract Class getJobConfigurationClass(MJob.Type jobType);
  public abstract Importer getImporter();
  public abstract Exporter getExporter();
  public abstract Validator getValidator();
  public abstract MetadataUpgrader getMetadataUpgrader();

The ``getImporter`` method returns Importer_ instance
which is a placeholder for the modules needed for import.

The ``getExporter`` method returns Exporter_ instance
which is a placeholder for the modules needed for export.

Methods such as ``getBundle`` , ``getConnectionConfigurationClass`` ,
``getJobConfigurationClass`` and ``getValidator``
are concerned to `Connector configurations`_ .


Importer
========

Connector's ``getImporter`` method returns ``Importer`` instance
which is a placeholder for the modules needed for import
such as Partitioner_ and Extractor_ .
Built-in ``GenericJdbcConnector`` defines ``Importer`` like this.
::

  private static final Importer IMPORTER = new Importer(
      GenericJdbcImportInitializer.class,
      GenericJdbcImportPartitioner.class,
      GenericJdbcImportExtractor.class,
      GenericJdbcImportDestroyer.class);
  
  ...
  
  @Override
  public Importer getImporter() {
    return IMPORTER;
  }


Extractor
---------

Extractor (E for ETL) extracts data from external database and
writes it to Sqoop framework for import.

Extractor must overrides ``extract`` method.
::

  public abstract void extract(ExtractorContext context,
                               ConnectionConfiguration connectionConfiguration,
                               JobConfiguration jobConfiguration,
                               Partition partition);

The ``extract`` method extracts data from database in some way and
writes it to ``DataWriter`` (provided by context) as `Intermediate representation`_ .

Extractor must iterates in the ``extract`` method until the data from database exhausts.
::

  while (resultSet.next()) {
    ...
    context.getDataWriter().writeArrayRecord(array);
    ...
  }


Partitioner
-----------

Partitioner creates ``Partition`` instances based on configurations.
The number of ``Partition`` instances is decided
based on the value users specified as the numbers of ectractors
in job configuration.

``Partition`` instances are passed to Extractor_ as the argument of ``extract`` method.
Extractor_ determines which portion of the data to extract by Partition.

There is no actual convention for Partition classes
other than being actually ``Writable`` and ``toString()`` -able.
::

  public abstract class Partition {
    public abstract void readFields(DataInput in) throws IOException;
    public abstract void write(DataOutput out) throws IOException;
    public abstract String toString();
  }

Connectors can define the design of ``Partition`` on their own.


Initializer and Destroyer
-------------------------

Initializer is instantiated before the submission of MapReduce job
for doing preparation such as adding dependent jar files.

Destroyer is instantiated after MapReduce job is finished for clean up.


Exporter
========

Connector's ``getExporter`` method returns ``Exporter`` instance
which is a placeholder for the modules needed for export
such as Loader_ .
Built-in ``GenericJdbcConnector`` defines ``Exporter`` like this.
::

  private static final Exporter EXPORTER = new Exporter(
      GenericJdbcExportInitializer.class,
      GenericJdbcExportLoader.class,
      GenericJdbcExportDestroyer.class);
  
  ...
  
  @Override
  public Exporter getExporter() {
    return EXPORTER;
  }


Loader
------

Loader (L for ETL) receives data from Sqoop framework and
loads it to external database.

Loader must overrides ``load`` method.
::

  public abstract void load(LoaderContext context,
                            ConnectionConfiguration connectionConfiguration,
                            JobConfiguration jobConfiguration) throws Exception;

The ``load`` method reads data from ``DataReader`` (provided by context)
in `Intermediate representation`_ and loads it to database in some way.

Loader must iterates in the ``load`` method until the data from ``DataReader`` exhausts.
::

  while ((array = context.getDataReader().readArrayRecord()) != null) {
    ...
  }


Initializer and Destroyer
-------------------------

Initializer is instantiated before the submission of MapReduce job
for doing preparation such as adding dependent jar files.

Destroyer is instantiated after MapReduce job is finished for clean up.


Connector Configurations
++++++++++++++++++++++++

Connector specifications
========================

Framework of the Sqoop loads definitions of connectors
from the file named ``sqoopconnector.properties``
which each connector implementation provides.
::

  # Generic JDBC Connector Properties
  org.apache.sqoop.connector.class = org.apache.sqoop.connector.jdbc.GenericJdbcConnector
  org.apache.sqoop.connector.name = generic-jdbc-connector


Configurations
==============

Implementation of ``SqoopConnector`` overrides methods such as
``getConnectionConfigurationClass`` and ``getJobConfigurationClass``
returning configuration class.
::

  @Override
  public Class getConnectionConfigurationClass() {
    return ConnectionConfiguration.class;
  }

  @Override
  public Class getJobConfigurationClass(MJob.Type jobType) {
    switch (jobType) {
      case IMPORT:
        return ImportJobConfiguration.class;
      case EXPORT:
        return ExportJobConfiguration.class;
      default:
        return null;
    }
  }

Configurations are represented
by models defined in ``org.apache.sqoop.model`` package.
Annotations such as
``ConfigurationClass`` , ``FormClass`` , ``Form`` and ``Input``
are provided for defining configurations of each connectors
using these models.

``ConfigurationClass`` is place holder for ``FormClasses`` .
::

  @ConfigurationClass
  public class ConnectionConfiguration {

    @Form public ConnectionForm connection;

    public ConnectionConfiguration() {
      connection = new ConnectionForm();
    }
  }

Each ``FormClass`` defines names and types of configs.
::

  @FormClass
  public class ConnectionForm {
    @Input(size = 128) public String jdbcDriver;
    @Input(size = 128) public String connectionString;
    @Input(size = 40)  public String username;
    @Input(size = 40, sensitive = true) public String password;
    @Input public Map<String, String> jdbcProperties;
  }


ResourceBundle
==============

Resources used by client user interfaces are defined in properties file.
::

  # jdbc driver
  connection.jdbcDriver.label = JDBC Driver Class
  connection.jdbcDriver.help = Enter the fully qualified class name of the JDBC \
                     driver that will be used for establishing this connection.

  # connect string
  connection.connectionString.label = JDBC Connection String
  connection.connectionString.help = Enter the value of JDBC connection string to be \
                     used by this connector for creating connections.

  ...

Those resources are loaded by ``getBundle`` method of connector.
::

  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
    GenericJdbcConnectorConstants.RESOURCE_BUNDLE_NAME, locale);
  }


Validator
=========

Validator validates configurations set by users.


Internal of Sqoop2 MapReduce Job
++++++++++++++++++++++++++++++++

Sqoop 2 provides common MapReduce modules such as ``SqoopMapper`` and ``SqoopReducer``
for the both of import and export.

- For import, ``Extractor`` provided by connector extracts data from databases,
  and ``Loader`` provided by Sqoop2 loads data into Hadoop.

- For export, ``Extractor`` provided by Sqoop2 exracts data from Hadoop,
  and ``Loader`` provided by connector loads data into databases.

The diagram below describes the initialization phase of IMPORT job.
``SqoopInputFormat`` create splits using ``Partitioner`` .
::

      ,----------------.          ,-----------.
      |SqoopInputFormat|          |Partitioner|
      `-------+--------'          `-----+-----'
   getSplits  |                         |
  ----------->|                         |
              |      getPartitions      |
              |------------------------>|
              |                         |         ,---------.
              |                         |-------> |Partition|
              |                         |         `----+----'
              |<- - - - - - - - - - - - |              |
              |                         |              |          ,----------.
              |-------------------------------------------------->|SqoopSplit|
              |                         |              |          `----+-----'

The diagram below describes the map phase of IMPORT job.
``SqoopMapper`` invokes extractor's ``extract`` method.
::

      ,-----------.
      |SqoopMapper|
      `-----+-----'
     run    |
  --------->|                                   ,-------------.
            |---------------------------------->|MapDataWriter|
            |                                   `------+------'
            |                ,---------.               |
            |--------------> |Extractor|               |
            |                `----+----'               |
            |      extract        |                    |
            |-------------------->|                    |
            |                     |                    |
           read from DB           |                    |
  <-------------------------------|      write*        |
            |                     |------------------->|
            |                     |                    |           ,----.
            |                     |                    |---------->|Data|
            |                     |                    |           `-+--'
            |                     |                    |
            |                     |                    |      context.write
            |                     |                    |-------------------------->

The diagram below decribes the reduce phase of EXPORT job.
``OutputFormat`` invokes loader's ``load`` method (via ``SqoopOutputFormatLoadExecutor`` ).
::

    ,-------.  ,---------------------.
    |Reducer|  |SqoopNullOutputFormat|
    `---+---'  `----------+----------'
        |                 |   ,-----------------------------.
        |                 |-> |SqoopOutputFormatLoadExecutor|
        |                 |   `--------------+--------------'        ,----.
        |                 |                  |---------------------> |Data|
        |                 |                  |                       `-+--'
        |                 |                  |   ,-----------------.   |
        |                 |                  |-> |SqoopRecordWriter|   |
      getRecordWriter     |                  |   `--------+--------'   |
  ----------------------->| getRecordWriter  |            |            |
        |                 |----------------->|            |            |     ,--------------.
        |                 |                  |-----------------------------> |ConsumerThread|
        |                 |                  |            |            |     `------+-------'
        |                 |<- - - - - - - - -|            |            |            |    ,------.
  <- - - - - - - - - - - -|                  |            |            |            |--->|Loader|
        |                 |                  |            |            |            |    `--+---'
        |                 |                  |            |            |            |       |
        |                 |                  |            |            |            | load  |
   run  |                 |                  |            |            |            |------>|
  ----->|                 |     write        |            |            |            |       |
        |------------------------------------------------>| setContent |            | read* |
        |                 |                  |            |----------->| getContent |<------|
        |                 |                  |            |            |<-----------|       |
        |                 |                  |            |            |            | - - ->|
        |                 |                  |            |            |            |       | write into DB
        |                 |                  |            |            |            |       |-------------->



.. _`Intermediate representation`: https://cwiki.apache.org/confluence/display/SQOOP/Sqoop2+Intermediate+representation
