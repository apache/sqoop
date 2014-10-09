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

The connector provides the facilities to interact with external data sources.
The connector can read from, or write to, a data source.

When do we add a new connector?
===============================
You add a new connector when you need to extract data from a new data source, or load
data to a new target.
In addition to the connector API, Sqoop 2 also has an engine interface.
At the moment the only engine is MapReduce, but we may support additional engines in the future.
Since many parallel execution engines are capable of reading/writing data
there may be a question of whether support for specific data stores should be done
through a new connector or new engine.

**Our guideline is:** Connectors should manage all data extract/load. Engines manage job
life cycles. If you need to support a new data store and don't care how jobs run -
you are looking to add a connector.


Connector Implementation
++++++++++++++++++++++++

The ``SqoopConnector`` class defines functionality
which must be provided by Connectors.
Each Connector must extend ``SqoopConnector`` and override the methods shown below.
::

  public abstract String getVersion();
  public abstract ResourceBundle getBundle(Locale locale);
  public abstract Class getLinkConfigurationClass();
  public abstract Class getJobConfigurationClass(Direction direction);
  public abstract From getFrom();
  public abstract To getTo();
  public abstract Validator getValidator();
  public abstract MetadataUpgrader getMetadataUpgrader();

Connectors can optionally override the following methods:
::

  public List<Direction> getSupportedDirections();


The ``getFrom`` method returns From_ instance
which is a placeholder for the modules needed to read from a data source.

The ``getTo`` method returns Exporter_ instance
which is a placeholder for the modules needed to write to a data source.

Methods such as ``getBundle`` , ``getConnectionConfigurationClass`` ,
``getJobConfigurationClass`` and ``getValidator``
are concerned to `Connector configurations`_ .

The ``getSupportedDirections`` method returns a list of directions
that a connector supports. This should be some subset of:
::

  public List<Direction> getSupportedDirections() {
      return Arrays.asList(new Direction[]{
          Direction.FROM,
          Direction.TO
      });
  }


From
====

The connector's ``getFrom`` method returns ``From`` instance
which is a placeholder for the modules needed for reading
from a data source. Modules such as Partitioner_ and Extractor_ .
The built-in ``GenericJdbcConnector`` defines ``From`` like this.
::

  private static final From FROM = new From(
        GenericJdbcFromInitializer.class,
        GenericJdbcPartitioner.class,
        GenericJdbcExtractor.class,
        GenericJdbcFromDestroyer.class);
  
  ...
  
  @Override
  public From getFrom() {
    return FROM;
  }


Extractor
---------

Extractor (E for ETL) extracts data from external database.

Extractor must overrides ``extract`` method.
::

  public abstract void extract(ExtractorContext context,
                               ConnectionConfiguration connectionConfiguration,
                               JobConfiguration jobConfiguration,
                               Partition partition);

The ``extract`` method extracts data from database in some way and
writes it to ``DataWriter`` (provided by context) as `Intermediate representation`_ .

Extractors use Writer's provided by the ExtractorContext to send a record through the
framework.
::

  context.getDataWriter().writeArrayRecord(array);

The extractor must iterate through the entire dataset in the ``extract`` method.
::

  while (resultSet.next()) {
    ...
    context.getDataWriter().writeArrayRecord(array);
    ...
  }


Partitioner
-----------

The Partitioner creates ``Partition`` instances based on configurations.
The number of ``Partition`` instances is decided
based on the value users specified as the numbers of extractors
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


To
==

The Connector's ``getTo`` method returns a ``To`` instance
which is a placeholder for the modules needed for writing
to a data source such as Loader_ .
The built-in ``GenericJdbcConnector`` defines ``To`` like this.
::

  private static final To TO = new To(
        GenericJdbcToInitializer.class,
        GenericJdbcLoader.class,
        GenericJdbcToDestroyer.class);
  
  ...
  
  @Override
  public To getTo() {
    return TO;
  }


Loader
------

A loader (L for ETL) receives data from the Sqoop framework and
loads it to an external database.

Loaders must overrides ``load`` method.
::

  public abstract void load(LoaderContext context,
                            ConnectionConfiguration connectionConfiguration,
                            JobConfiguration jobConfiguration) throws Exception;

The ``load`` method reads data from ``DataReader`` (provided by context)
in `Intermediate representation`_ and loads it to database in some way.

Loader must iterate in the ``load`` method until the data from ``DataReader`` is exhausted.
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

Sqoop loads definitions of connectors
from the file named ``sqoopconnector.properties``
which each connector implementation provides.
::

  # Generic JDBC Connector Properties
  org.apache.sqoop.connector.class = org.apache.sqoop.connector.jdbc.GenericJdbcConnector
  org.apache.sqoop.connector.name = generic-jdbc-connector


Configurations
==============

Implementations of ``SqoopConnector`` overrides methods such as
``getConnectionConfigurationClass`` and ``getJobConfigurationClass``
returning configuration class.
::

  @Override
  public Class getConnectionConfigurationClass() {
    return ConnectionConfiguration.class;
  }

  @Override
  public Class getJobConfigurationClass(Direction direction) {
    switch (direction) {
      case FROM:
        return FromJobConfiguration.class;
      case TO:
        return ToJobConfiguration.class;
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

``ConfigurationClass`` is a place holder for ``FormClasses`` .
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

Sqoop 2 provides common MapReduce modules such as ``SqoopMapper`` and ``SqoopReducer``.

When reading from a data source, the ``Extractor`` provided by the FROM connector extracts data from a database,
and the ``Loader``, provided by the TO connector, loads data into another database.

The diagram below describes the initialization phase of a job.
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

The diagram below describes the map phase of a job.
``SqoopMapper`` invokes FROM connector's extractor's ``extract`` method.
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

The diagram below decribes the reduce phase of a job.
``OutputFormat`` invokes TO connector's loader's ``load`` method (via ``SqoopOutputFormatLoadExecutor`` ).
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
