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

This document describes how to implement a connector in the Sqoop 2 using the code sample from one of the built-in connectors ( ``GenericJdbcConnector`` ) as a reference. Sqoop 2 jobs support extraction from and/or loading to different data sources. Sqoop 2 connectors encapsulate the job lifecyle operations for extracting and/or loading data from and/or to
different data sources. Each connector will primarily focus on a particular data source and its custom implementation for optimally reading and/or writing data in a distributed environment.

.. contents::

What is a Sqoop Connector?
++++++++++++++++++++++++++

Connectors provide the facility to interact with many data sources and thus can be used as a means to transfer data between them in Sqoop. The connector implementation will provide logic to read from and/or write to a data source that it represents. For instance the ( ``GenericJdbcConnector`` ) encapsulates the logic to read from and/or write to jdbc enabled relational data sources. The connector part that enables reading from a data source and transferring this data to internal Sqoop format is called the FROM and the part that enables writng data to a data source by transferring data from Sqoop format is called TO. In order to interact with these data sources, the connector will provide one or many config classes and input fields within it.

Broadly we support two main config types for connectors, link type represented by the enum ``ConfigType.LINK`` and job type represented by the enum ``ConfigType.JOB``. Link config represents the properties to physically connect to the data source. Job config represent the properties that are required to invoke reading from and/or writing to particular dataset in the data source it connects to. If a connector supports both reading from and writing to, it will provide the ``FromJobConfig`` and ``ToJobConfig`` objects. Each of these config objects are custom to each connector and can have one or more inputs associated with each of the Link, FromJob and ToJob config types. Hence we call the connectors as configurables i.e an entity that can provide configs for interacting with the data source it represents. As the connectors evolve over time to support new features in their data sources, the configs and inputs will change as well. Thus the connector API also provides methods for upgrading the config and input names and data related to these data sources across different versions.

The connectors implement logic for various stages of the extract/load process using the connector API described below. While extracting/reading data from the data-source the main stages are ``Initializer``, ``Partitioner``, ``Extractor`` and ``Destroyer``. While loading/writitng data to the data source the main stages currently supported are ``Initializer``, ``Loader`` and ``Destroyer``. Each stage has its unique set of responsibilities that are explained in detail below. Since connectors understand the internals of the data source they represent, they work in tandem with the sqoop supported execution engines such as MapReduce or Spark (in future) to accomplish this process in a most optimal way.

When do we add a new connector?
===============================
You add a new connector when you need to extract/read data from a new data source, or load/write
data into a new data source that is not supported yet in Sqoop 2.
In addition to the connector API, Sqoop 2 also has an submission and execution engine interface.
At the moment the only supported engine is MapReduce, but we may support additional engines in the future such as Spark. Since many parallel execution engines are capable of reading/writing data, there may be a question of whether adding support for a new data source should be done through the connector or the execution engine API.

**Our guideline are as follows:** Connectors should manage all data extract(reading) from and/or load(writing) into a data source. Submission and execution engine together manage the job submission and execution life cycle to read/write data from/to data sources in the most optimal way possible. If you need to support a new data store and details of linking to it and don't care how the process of reading/writing from/to happens then you are looking to add a connector and you should continue reading the below Connector API details to contribute new connectors to Sqoop 2.


Connector Implementation
++++++++++++++++++++++++

The ``SqoopConnector`` class defines an API for the connectors that must be implemented by the connector developers. Each Connector must extend ``SqoopConnector`` and override the methods shown below.
::

  public abstract String getVersion();
  public abstract ResourceBundle getBundle(Locale locale);
  public abstract Class getLinkConfigurationClass();
  public abstract Class getJobConfigurationClass(Direction direction);
  public abstract From getFrom();
  public abstract To getTo();
  public abstract ConnectorConfigurableUpgrader getConfigurableUpgrader()

Connectors can optionally override the following methods:
::

  public List<Direction> getSupportedDirections();
  public Class<? extends IntermediateDataFormat<?>> getIntermediateDataFormat()

The ``getVersion`` method returns the current version of the connector
It is important to provide a unique identifier every time a connector jar is released externally.
In case of the Sqoop built-in connectors, the version refers to the Sqoop build/release version. External
connectors can also use the same or similar mechanism to set this version. The version number is critical for
the connector upgrade logic used in Sqoop

::

   @Override
    public String getVersion() {
     return VersionInfo.getBuildVersion();
    }


The ``getFrom`` method returns From_ instance
which is a ``Transferable`` entity that encapsulates the operations
needed to read from the data source that the connector represents.

The ``getTo`` method returns To_ instance
which is a ``Transferable`` entity that encapsulates the operations
needed to write to the data source that the connector represents.

Methods such as ``getBundle`` , ``getLinkConfigurationClass`` , ``getJobConfigurationClass``
are related to `Configurations`_

Since a connector represents a data source and it can support one of the two directions, either reading FROM its data source or writing to its data souurce or both, the ``getSupportedDirections`` method returns a list of directions that a connector will implement. This should be a subset of the values in the ``Direction`` enum we provide:
::

  public List<Direction> getSupportedDirections() {
      return Arrays.asList(new Direction[]{
          Direction.FROM,
          Direction.TO
      });
  }


From
====

The ``getFrom`` method returns From_ instance which is a ``Transferable`` entity that encapsulates the operations needed to read from the data source the connector represents. The built-in ``GenericJdbcConnector`` defines ``From`` like this.
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

Initializer and Destroyer
-------------------------
.. _Initializer:
.. _Destroyer:

Initializer is instantiated before the submission of sqoop job to the execution engine and doing preparations such as connecting to the data source, creating temporary tables or adding dependent jar files. Initializers are executed as the first step in the sqoop job lifecyle. Here is the ``Initializer`` API.
::

  public abstract void initialize(InitializerContext context, LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration);

  public List<String> getJars(InitializerContext context, LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration){
       return new LinkedList<String>();
      }

  public abstract Schema getSchema(InitializerContext context, LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration) {
         return new NullSchema();
      }

In addition to the initialize() method where the job execution preparation activities occur, the ``Initializer`` can also implement the getSchema() method for the directions ``FROM`` and ``TO`` that it supports.

The getSchema() method is used by the sqoop system to match the data extracted/read by the ``From`` instance of connector data source with the data loaded/written to the ``To`` instance of the connector data source. In case of a relational database or columnar database, the returned Schema object will include collection of columns with their data types. If the data source is schema-less, such as a file, a default ``NullSchema`` will be used (i.e a Schema object without any columns).

NOTE: Sqoop 2 currently does not support extract and load between two connectors that represent schema-less data sources. We expect that atleast the ``From`` instance of the connector or the ``To`` instance of the connector in the sqoop job will have a schema. If both ``From`` and ``To`` have a associated non empty schema, Sqoop 2 will load data by column name, i.e, data in column "A" in ``From`` instance of the connector for the job will be loaded to column "A" in the ``To`` instance of the connector for that job.


``Destroyer`` is instantiated after the execution engine finishes its processing. It is the last step in the sqoop job lifecyle, so pending clean up tasks such as dropping temporary tables and closing connections. The term destroyer is a little misleading. It represents the phase where the final output commits to the data source can also happen in case of the ``TO`` instance of the connector code.

Partitioner
-----------

The ``Partitioner`` creates ``Partition`` instances ranging from 1..N. The N is driven by a configuration as well. The default set of partitions created is set to 10 in the sqoop code. Here is the ``Partitioner`` API

``Partitioner`` must implement the ``getPartitions`` method in the ``Partitioner`` API.

::

  public abstract List<Partition> getPartitions(PartitionerContext context,
      LinkConfiguration linkConfiguration, FromJobConfiguration jobConfiguration);

``Partition`` instances are passed to Extractor_ as the argument of ``extract`` method.
Extractor_ determines which portion of the data to extract by a given partition.

There is no actual convention for Partition classes other than being actually ``Writable`` and ``toString()`` -able. Here is the ``Partition`` API
::

  public abstract class Partition {
    public abstract void readFields(DataInput in) throws IOException;
    public abstract void write(DataOutput out) throws IOException;
    public abstract String toString();
  }

Connectors can implement custom ``Partition`` classes. ``GenericJdbcPartitioner`` is one such example. It returns the ``GenericJdbcPartition`` objects.

Extractor
---------

Extractor (E for ETL) extracts data from a given data source
``Extractor`` must implement the ``extract`` method in the ``Extractor`` API.
::

  public abstract void extract(ExtractorContext context,
                               LinkConfiguration linkConfiguration,
                               JobConfiguration jobConfiguration,
                               SqoopPartition partition);

The ``extract`` method extracts data from the data source using the link and job configuration properties and writes it to the ``SqoopMapDataWriter`` (provided in the extractor context given to the extract method).
The ``SqoopMapDataWriter`` has the ``SqoopWritable`` thats holds the data read from the data source in the `Intermediate Data Format representation`_

Extractors use Writer's provided by the ExtractorContext to send a record through the sqoop system.
::

  context.getDataWriter().writeArrayRecord(array);

The extractor must iterate through the given partition in the ``extract`` method.
::

  while (resultSet.next()) {
    ...
    context.getDataWriter().writeArrayRecord(array);
    ...
  }


To
==

The ``getTo`` method returns ``TO`` instance which is a ``Transferable`` entity that encapsulates the operations needed to wtite data to the data source the connector represents. The built-in ``GenericJdbcConnector`` defines ``To`` like this.
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


Initializer and Destroyer
-------------------------

Initializer_ and Destroyer_ of a ``To`` instance are used in a similar way to those of a ``From`` instance.
Refer to the previous section for more details.


Loader
------

A loader (L for ETL) receives data from the ``From`` instance of the sqoop connector associated with the sqoop job and then loads it to an ``TO`` instance of the connector associated with the same sqoop job

``Loader`` must implement ``load`` method of the ``Loader`` API
::

  public abstract void load(LoaderContext context,
                            ConnectionConfiguration connectionConfiguration,
                            JobConfiguration jobConfiguration) throws Exception;

The ``load`` method reads data from ``SqoopOutputFormatDataReader`` (provided in the loader context of the load methods). It reads the data in the `Intermediate Data Format representation`_ and loads it to the data source.

Loader must iterate in the ``load`` method until the data from ``DataReader`` is exhausted.
::

  while ((array = context.getDataReader().readArrayRecord()) != null) {
    ...
  }

NOTE: we do not yet support a stage for connector developers to control how to balance the loading/writitng of data across the mutiple loaders. In future we may be adding this to the connector API to have custom logic to balance the loading across multiple reducers.

Sqoop Connector Identifier : sqoopconnector.properties
======================================================

Every Sqoop 2 connector needs to have a sqoopconnector.properties in the packaged jar to be identified by Sqoop.
A typical ``sqoopconnector.properties`` for a sqoop2 connector looks like below

::

 # Sqoop Foo Connector Properties
 org.apache.sqoop.connector.class = org.apache.sqoop.connector.foo.FooConnector
 org.apache.sqoop.connector.name = sqoop-foo-connector

If the above file does not exist, then Sqoop will not load this jar and thus cannot be registered into Sqoop repository for creating Sqoop jobs


Sqoop Connector Build-time Dependencies
=======================================

Sqoop provides the connector-sdk module identified by the package:``org.apache.sqoop.connector`` It provides the public facing apis for the external connectors
to extend from. It also provides common utilities that the connectors can utilize for converting data to and from the sqoop intermediate data format

The common-test module identified by the package  ``org.apache.sqoop.common.test`` provides utilities used related to the built-in connectors such as the JDBC, HDFS,
and Kafka connectors that can be used by the external connectors for creating the end-end integration test for sqoop jobs

The test module identified by the package ``org.apache.sqoop.test`` provides various minicluster utilites the integration tests can extend from to run
 a sqoop job with the given sqoop connector either using it as a ``FROM`` or ``TO`` data-source

Hence the pom.xml for the sqoop kite connector built using the kite-sdk  might look something like below

::

   <dependencies>
    <!-- Sqoop modules -->
    <dependency>
      <groupId>org.apache.sqoop</groupId>
      <artifactId>connector-sdk</artifactId>
    </dependency>

    <!-- Testing specified modules -->
    <dependency>
      <groupId>org.testng</groupId>
      <artifactId>testng</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.mockito</groupId>
      <artifactId>mockito-all</artifactId>
      <scope>test</scope>
    </dependency>
     <dependency>
       <groupId>org.apache.sqoop</groupId>
       <artifactId>sqoop-common-test</artifactId>
     </dependency>

     <dependency>
       <groupId>org.apache.sqoop</groupId>
       <artifactId>test</artifactId>
     </dependency>
    <!-- Connector required modules -->
    <dependency>
      <groupId>org.kitesdk</groupId>
      <artifactId>kite-data-core</artifactId>
    </dependency>
    ....
  </dependencies>

Configurables
+++++++++++++

Configurable registration
=========================
One of the currently supported configurable in Sqoop are the connectors. Sqoop 2 registers definitions of connectors from the file named ``sqoopconnector.properties`` which each connector implementation should provide to become available in Sqoop.
::

  # Generic JDBC Connector Properties
  org.apache.sqoop.connector.class = org.apache.sqoop.connector.jdbc.GenericJdbcConnector
  org.apache.sqoop.connector.name = generic-jdbc-connector


Configurations
==============

Implementations of ``SqoopConnector`` overrides methods such as ``getLinkConfigurationClass`` and ``getJobConfigurationClass`` returning configuration class.
::

  @Override
  public Class getLinkConfigurationClass() {
    return LinkConfiguration.class;
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

Configurations are represented by annotations defined in ``org.apache.sqoop.model`` package.
Annotations such as ``ConfigurationClass`` , ``ConfigClass`` , ``Config`` and ``Input``
are provided for defining configuration objects for each connector.

``@ConfigurationClass`` is a marker annotation for ``ConfigurationClasses``  that hold a group or lis of ``ConfigClasses`` annotated with the marker ``@ConfigClass``
::

  @ConfigurationClass
  public class LinkConfiguration {

    @Config public LinkConfig linkConfig;

    public LinkConfiguration() {
      linkConfig = new LinkConfig();
    }
  }

Each ``ConfigClass`` defines the different inputs it exposes for the link and job configs. These inputs are annotated with ``@Input`` and the user will be asked to fill in when they create a sqoop job and choose to use this instance of the connector for either the ``From`` or ``To`` part of the job.

::

    @ConfigClass(validators = {@Validator(LinkConfig.ConfigValidator.class)})
    public class LinkConfig {
      @Input(size = 128, validators = {@Validator(NotEmpty.class), @Validator(ClassAvailable.class)} )
      @Input(size = 128) public String jdbcDriver;
      @Input(size = 128) public String connectionString;
      @Input(size = 40)  public String username;
      @Input(size = 40, sensitive = true) public String password;
      @Input public Map<String, String> jdbcProperties;
    }

Each ``ConfigClass`` and the  inputs within the configs annotated with ``Input`` can specifiy validators via the ``@Validator`` annotation described below.


Configs and Inputs
==================================
As discussed above, ``Input`` provides a way to express the type of config parameter exposed. In addition it allows connector developer to add attributes
that describe how the input will be used in the sqoop job. Here are the list of the supported attributes


Inputs associated with the link configuration include:

+-----------------------------+---------+-----------------------------------------------------------------------+-------------------------------------------------+
| Attribute                   | Type    | Description                                                           | Example                                         |
+=============================+=========+=======================================================================+=================================================+
| size                        | Integer |Describes the maximum size of the attribute value .                    |@Input(size = 128) public String driver          |
+-----------------------------+---------+-----------------------------------------------------------------------+-------------------------------------------------+
| sensitive                   | Boolean |Describes if the input value should be hidden from display             |@Input(sensitive = true) public String password  |
+-----------------------------+---------+-----------------------------------------------------------------------+-------------------------------------------------+
| editable                    | Enum    |Describes the roles that can edit the value of this input              |@Input(editable = ANY) public String value       |
+-----------------------------+---------+-----------------------------------------------------------------------+-------------------------------------------------+
| overrides                   | String  |Describes a list of other inputs this input can override in this config|@Input(overrides ="value") public String lvalue  |
+-----------------------------+---------+-----------------------------------------------------------------------+-------------------------------------------------+


``Editable`` Attribute: Possible values for the Enum InputEditable are USER_ONLY, CONNECTOR_ONLY, ANY. If an input says editable by USER_ONLY, then the connector code during the
job run or upgrade cannot update the config input value. Similarly for a CONNECTOR_ONLY, user cannot update its value via the rest api or shell command line.

``Overrides`` Attribute: USER_ONLY input attribute values cannot be overriden by other inputs.

Empty Configuration
-------------------
If a connector does not have any configuration inputs to specify for the ``ConfigType.LINK`` or ``ConfigType.JOB`` it is recommended to return the ``EmptyConfiguration`` class in the ``getLinkConfigurationClass()`` or ``getJobConfigurationClass(..)`` methods.
::

   @ConfigurationClass
   public class EmptyConfiguration { }


Configuration ResourceBundle
============================

The config and its corresponding input names, the input field description are represented in the config resource bundle defined per connector.
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

Those resources are loaded by ``getBundle`` method of the ``SqoopConnector.``
::

  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
    GenericJdbcConnectorConstants.RESOURCE_BUNDLE_NAME, locale);
  }


Validations for Configs and Inputs
==================================

Validators validate the config objects and the inputs associated with the config objects. For config objects themselves we encourage developers to write custom valdiators for both the link and job config types.

::

   @Input(size = 128, validators = {@Validator(value = StartsWith.class, strArg = "jdbc:")} )

   @Input(size = 255, validators = { @Validator(NotEmpty.class) })

Sqoop 2 provides a list of standard input validators that can be used by different connectors for the link and job type configuration inputs.

::

    public class NotEmpty extends AbstractValidator<String> {
    @Override
    public void validate(String instance) {
      if (instance == null || instance.isEmpty()) {
       addMessage(Status.ERROR, "Can't be null nor empty");
      }
     }
    }

The validation logic is executed when users creating the sqoop jobs input values for the link and job configs associated with the ``From`` and ``To`` instances of the connectors associated with the job.


Loading External Connectors
+++++++++++++++++++++++++++

Loading new connector say sqoop-foo-connector to the sqoop2, here are the steps to follow

1. Create a ``sqoop-foo-connector.jar``. Make sure the jar contains the ``sqoopconnector.properties`` for it to be picked up by Sqoop

2. Add this jar to the a folder on your installation machine and update the path to this folder in the sqoop.properties located under the ``server/conf`` directory under the Sqoop2  for the key ``org.apache.sqoop.connector.external.loadpath``

::

 #
 # External connectors load path
 # "/path/to/external/connectors/": Add all the connector JARs in the specified folder
 #
 org.apache.sqoop.connector.external.loadpath=/path/to/connector

3. Start the Sqoop 2 server and while initializing the server this jar should be loaded into the Sqoop 2's class path and registered into the Sqoop 2 repository



Sqoop 2 MapReduce Job Execution Lifecycle with Connector API
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Sqoop 2 provides MapReduce utilities such as ``SqoopMapper`` and ``SqoopReducer`` that aid sqoop job execution.

Note: Any class prefixed with Sqoop is a internal sqoop class provided for MapReduce and is not part of the conenector API. These internal classes work with the custom implementations of ``Extractor``, ``Partitioner`` in the ``From`` instance and ``Loader`` in the ``To`` instance of the connector.

When reading from a data source, the ``Extractor`` provided by the ``From`` instance of the connector extracts data from a corresponding data source it represents and the ``Loader``, provided by the TO instance of the connector, loads data into the data source it represents.

The diagram below describes the initialization phase of a job.
``SqoopInputFormat`` create splits using ``Partitioner``.
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
``SqoopMapper`` invokes ``From`` connector's extractor's ``extract`` method.
::

      ,-----------.
      |SqoopMapper|
      `-----+-----'
     run    |
  --------->|                                   ,------------------.
            |---------------------------------->|SqoopMapDataWriter|
            |                                   `------+-----------'
            |                ,---------.               |
            |--------------> |Extractor|               |
            |                `----+----'               |
            |      extract        |                    |
            |-------------------->|                    |
            |                     |                    |
           read from Data Source  |                    |
  <-------------------------------|      write*        |
            |                     |------------------->|
            |                     |                    |           ,-------------.
            |                     |                    |---------->|SqoopWritable|
            |                     |                    |           `----+--------'
            |                     |                    |                |
            |                     |                    |                |  context.write(writable, ..)
            |                     |                    |                |---------------------------->

The diagram below decribes the reduce phase of a job.
``OutputFormat`` invokes ``To`` connector's loader's ``load`` method (via ``SqoopOutputFormatLoadExecutor`` ).
::

    ,------------.  ,---------------------.
    |SqoopReducer|  |SqoopNullOutputFormat|
    `---+--------'  `----------+----------'
        |                 |   ,-----------------------------.
        |                 |-> |SqoopOutputFormatLoadExecutor|
        |                 |   `--------------+--------------'              |
        |                 |                  |                             |
        |                 |                  |   ,-----------------.   ,-------------.
        |                 |                  |-> |SqoopRecordWriter|-->|SqoopWritable|
      getRecordWriter     |                  |   `--------+--------'   `---+---------'
  ----------------------->| getRecordWriter  |            |                |
        |                 |----------------->|            |                |     ,--------------.
        |                 |                  |---------------------------------->|ConsumerThread|
        |                 |                  |            |                |     `------+-------'
        |                 |<- - - - - - - - -|            |                |            |    ,------.
  <- - - - - - - - - - - -|                  |            |                |            |--->|Loader|
        |                 |                  |            |                |            |    `--+---'
        |                 |                  |            |                |            |       |
        |                 |                  |            |                |            | load  |
   run  |                 |                  |            |                |            |------>|
  ----->|                 |     write        |            |                |            |       |
        |------------------------------------------------>| setContent     |            | read* |
        |                 |                  |            |--------------->| getContent |<------|
        |                 |                  |            |                |<-----------|       |
        |                 |                  |            |                |            | - - ->|
        |                 |                  |            |                |            |       | write into Data Source
        |                 |                  |            |                |            |       |----------------------->

More details can be found in `Sqoop MR Execution Engine`_

.. _`Sqoop MR Execution Engine`: https://cwiki.apache.org/confluence/display/SQOOP/Sqoop+MR+Execution+Engine

.. _`Intermediate Data Format representation`: https://cwiki.apache.org/confluence/display/SQOOP/Sqoop2+Intermediate+representation
