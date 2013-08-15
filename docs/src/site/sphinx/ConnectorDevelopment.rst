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

This document describes you how to implement connector for Sqoop 2.


What is Connector?
++++++++++++++++++

Connector provides interaction with external databases.
Connector reads data from databases for import,
and write data to databases for export.
Interaction with Hadoop is taken cared by common modules of Sqoop 2 framework.


Connector Implementation
++++++++++++++++++++++++

The SqoopConnector class defines functionality
which must be provided by Connectors.
Each Connector must extends SqoopConnector and overrides methods shown below.
::

  public abstract String getVersion();
  public abstract ResourceBundle getBundle(Locale locale);
  public abstract Class getConnectionConfigurationClass();
  public abstract Class getJobConfigurationClass(MJob.Type jobType);
  public abstract Importer getImporter();
  public abstract Exporter getExporter();
  public abstract Validator getValidator();
  public abstract MetadataUpgrader getMetadataUpgrader();

The getImporter method returns Importer_ instance
which is a placeholder for the modules needed for import.

The getExporter method returns Exporter_ instance
which is a placeholder for the modules needed for export.

Methods such as getBundle, getConnectionConfigurationClass,
getJobConfigurationClass and getValidator
are concerned to `Connector configurations`_ .


Importer
========

Connector#getImporter method returns Importer instance
which is a placeholder for the modules needed for import
such as Partitioner_ and Extractor_ .
Built-in GenericJdbcConnector defines Importer like this.
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

Extractor must overrides extract method.
::

  public abstract void extract(ExtractorContext context,
                               ConnectionConfiguration connectionConfiguration,
                               JobConfiguration jobConfiguration,
                               Partition partition);

The extract method extracts data from database in some way and
writes it to DataWriter (provided by context) as `Intermediate representation`_ .

Extractor must iterates in the extract method until the data from database exhausts.
::

  while (resultSet.next()) {
    ...
    context.getDataWriter().writeArrayRecord(array);
    ...
  }


Partitioner
-----------

Partitioner creates Partition instances based on configurations.
The number of Partition instances is interpreted as the number of map tasks.
Partition instances are passed to Extractor_ as the argument of extract method.
Extractor_ determines which portion of the data to extract by Partition.

There is no actual convention for Partition classes
other than being actually Writable and toString()-able.
::

  public abstract class Partition {
    public abstract void readFields(DataInput in) throws IOException;
    public abstract void write(DataOutput out) throws IOException;
    public abstract String toString();
  }

Connectors can define the design of Partition on their own.


Initializer and Destroyer
-------------------------

Initializer is instantiated before the submission of MapReduce job
for doing preparation such as adding dependent jar files.

Destroyer is instantiated after MapReduce job is finished for clean up.


Exporter
========

Connector#getExporter method returns Exporter instance
which is a placeholder for the modules needed for export
such as Loader_ .
Built-in GenericJdbcConnector defines Exporter like this.
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

Loader must overrides load method.
::

  public abstract void load(LoaderContext context,
                            ConnectionConfiguration connectionConfiguration,
                            JobConfiguration jobConfiguration) throws Exception;

The load method reads data from DataReader (provided by context)
in `Intermediate representation`_ and loads it to database in some way.

Loader must iterates in the load method until the data from DataReader exhausts.
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

Configurations
==============

The definition of the configurations are represented
by models defined in org.apache.sqoop.model package.


ConnectionConfigurationClass
----------------------------


JobConfigurationClass
---------------------


ResourceBundle
==============

Resources for Configurations_ are stored in properties file
accessed by getBundle method of the Connector.


Validator
=========

Validator validates configurations set by users.


Internal of Sqoop2 MapReduce Job
++++++++++++++++++++++++++++++++

Sqoop 2 provides common MapReduce modules such as SqoopMapper and SqoopReducer
for the both of import and export.

- InputFormat create splits using Partitioner.

- SqoopMapper invokes Extractor's extract method.

- SqoopReducer do no actual works.

- OutputFormat invokes Loader's load method (via SqoopOutputFormatLoadExecutor).

.. todo: sequence diagram like figure.

For import, Extractor provided by Connector extracts data from databases,
and Loader provided by Sqoop2 loads data into Hadoop.

For export, Extractor provided Sqoop2 exracts data from Hadoop,
and Loader provided by Connector loads data into databases.


.. _`Intermediate representation`: https://cwiki.apache.org/confluence/display/SQOOP/Sqoop2+Intermediate+representation
