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
Sqoop Client API Guide
======================

This document will explain how to use Sqoop Client API with external application. Client API allows you to execute the functions of sqoop commands. It requires Sqoop Client JAR and its dependencies.

Client API is explained using Generic JDBC Connector properties. Before executing the application using the sqoop client API, check whether sqoop server is running.

Workflow
========

Given workflow has to be followed for executing a job in Sqoop server.

  1. Create connection using Connector ID (cid) - Creates connection and returns connection ID (xid)
  2. Create Job using Connection ID (xid)       - Create job and returns Job ID (jid)
  3. Job submission with Job ID (jid)           - Submit sqoop Job to server

Project Dependencies
====================
Here given maven dependency

::

  <dependency>
    <groupId>org.apache.sqoop</groupId>
      <artifactId>sqoop-client</artifactId>
      <version>${requestedVersion}</version>
  </dependency>

Initialization
==============

First initialize the SqoopClient class with server URL as argument.

::

  String url = "http://localhost:12000/sqoop/";
  SqoopClient client = new SqoopClient(url);

Server URL value can be modfied by setting value to setServerUrl(String) method

::

  client.setServerUrl(newUrl);


Connection
==========

Client API allows you to create, update and delete connection. For creating or updating connection requires Connector forms and Framwork Forms. User has to retrive the connector and framework forms, then update the values.

Create Connection
-----------------

First create a new connection by invoking newConnection(cid) method with connector ID and returns a MConnection object with dummy id. Then fill the connection and framework forms as given below. Invoke create connection with updated connection object.

::

  //Dummy connection object
  MConnection newCon = client.newConnection(1);

  //Get connection and framework forms. Set name for connection
  MConnectionForms conForms = newCon.getConnectorPart();
  MConnectionForms frameworkForms = newCon.getFrameworkPart();
  newCon.setName("MyConnection");

  //Set connection forms values
  conForms.getStringInput("connection.connectionString").setValue("jdbc:mysql://localhost/my");
  conForms.getStringInput("connection.jdbcDriver").setValue("com.mysql.jdbc.Driver");
  conForms.getStringInput("connection.username").setValue("root");
  conForms.getStringInput("connection.password").setValue("root");

  frameworkForms.getIntegerInput("security.maxConnections").setValue(0);

  Status status  = client.createConnection(newCon);
  if(status.canProceed()) {
   System.out.println("Created. New Connection ID : " +newCon.getPersistenceId());
  } else {
   System.out.println("Check for status and forms error ");
  }

status.canProceed() returns true if status is FINE or ACCEPTABLE. Above code has given status after validation of connector and framework forms.

On successful execution, new connection ID is assigned for the connection. getPersistenceId() method returns ID.
User can retrieve a connection using below methods

+----------------------------+--------------------------------------+
|   Method                   | Description                          |
+============================+======================================+
| ``getConnection(xid)``     | Returns a connection object.         |
+----------------------------+--------------------------------------+
| ``getConnections()``       | Returns list of connection object    |
+----------------------------+--------------------------------------+

List of status code
-------------------

+------------------+------------------------------------------------------------------------------------------------------------+
| Function         | Description                                                                                                |
+==================+============================================================================================================+
| ``FINE``         | There are no issues, no warnings.                                                                          |
+------------------+------------------------------------------------------------------------------------------------------------+
| ``ACCEPTABLE``   | Validated entity is correct enough to be processed. There might be some warnings, but no errors.           |
+------------------+------------------------------------------------------------------------------------------------------------+
| ``UNACCEPTABLE`` | There are serious issues with validated entity. We can't proceed until reported issues will be resolved.   |
+------------------+------------------------------------------------------------------------------------------------------------+

View Error or Warning message
-----------------------------

In case of any UNACCEPTABLE AND ACCEPTABLE status, user has to iterate the connector part forms and framework part forms for getting actual error or warning message. Below piece of code describe how to itereate over the forms for input message.

::

 printMessage(newCon.getConnectorPart().getForms());
 printMessage(newCon.getFrameworkPart().getForms());

 private static void printMessage(List<MForm> formList) {
   for(MForm form : formList) {
     List<MInput<?>> inputlist = form.getInputs();
     if (form.getValidationMessage() != null) {
       System.out.println("Form message: " + form.getValidationMessage());
     }
     for (MInput minput : inputlist) {
       if (minput.getValidationStatus() == Status.ACCEPTABLE) {
         System.out.println("Warning:" + minput.getValidationMessage());
       } else if (minput.getValidationStatus() == Status.UNACCEPTABLE) {
         System.out.println("Error:" + minput.getValidationMessage());
       }
     }
   }
 }

Job
===

A job object holds database configurations, input or output configurations and resources required for executing as a hadoop job. Create job object requires filling connector part and framework part forms.

Below given code shows how to create a import job

::

  String url = "http://localhost:12000/sqoop/";
  SqoopClient client = new SqoopClient(url);
  //Creating dummy job object
  MJob newjob = client.newJob(1, org.apache.sqoop.model.MJob.Type.IMPORT);
  MJobForms connectorForm = newjob.getConnectorPart();
  MJobForms frameworkForm = newjob.getFrameworkPart();

  newjob.setName("ImportJob");
  //Database configuration
  connectorForm.getStringInput("table.schemaName").setValue("");
  //Input either table name or sql
  connectorForm.getStringInput("table.tableName").setValue("table");
  //connectorForm.getStringInput("table.sql").setValue("select id,name from table where ${CONDITIONS}");
  connectorForm.getStringInput("table.columns").setValue("id,name");
  connectorForm.getStringInput("table.partitionColumn").setValue("id");
  //Set boundary value only if required
  //connectorForm.getStringInput("table.boundaryQuery").setValue("");

  //Output configurations
  frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
  frameworkForm.getEnumInput("output.outputFormat").setValue("TEXT_FILE");//Other option: SEQUENCE_FILE
  frameworkForm.getStringInput("output.outputDirectory").setValue("/output");

  //Job resources
  frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
  frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

  Status status = client.createJob(newjob);
  if(status.canProceed()) {
   System.out.println("New Job ID: "+ newjob.getPersistenceId());
  } else {
   System.out.println("Check for status and forms error ");
  }

  //Print errors or warnings
  printMessage(newjob.getConnectorPart().getForms());
  printMessage(newjob.getFrameworkPart().getForms());


Export job creation is same as import job, but only few input configuration changes

::

  String url = "http://localhost:12000/sqoop/";
  SqoopClient client = new SqoopClient(url);
  MJob newjob = client.newJob(1, org.apache.sqoop.model.MJob.Type.EXPORT);
  MJobForms connectorForm = newjob.getConnectorPart();
  MJobForms frameworkForm = newjob.getFrameworkPart();

  newjob.setName("ExportJob");
  //Database configuration
  connectorForm.getStringInput("table.schemaName").setValue("");
  //Input either table name or sql
  connectorForm.getStringInput("table.tableName").setValue("table");
  //connectorForm.getStringInput("table.sql").setValue("select id,name from table where ${CONDITIONS}");
  connectorForm.getStringInput("table.columns").setValue("id,name");

  //Input configurations
  frameworkForm.getStringInput("input.inputDirectory").setValue("/input");

  //Job resources
  frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
  frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

  Status status = client.createJob(newjob);
  if(status.canProceed()) {
    System.out.println("New Job ID: "+ newjob.getPersistenceId());
  } else {
    System.out.println("Check for status and forms error ");
  }

  //Print errors or warnings
  printMessage(newjob.getConnectorPart().getForms());
  printMessage(newjob.getFrameworkPart().getForms());

Managing connection and job
---------------------------
After creating connection or job object, you can update or delete a connection or job using given functions

+----------------------------------+------------------------------------------------------------------------------------+
|   Method                         | Description                                                                        |
+==================================+====================================================================================+
| ``updateConnection(connection)`` | Invoke update with connection object and check status for any errors or warnings   |
+----------------------------------+------------------------------------------------------------------------------------+
| ``deleteConnection(xid)``        | Delete connection. Deletes only if specified connection is used by any job         |
+----------------------------------+------------------------------------------------------------------------------------+
| ``updateJob(job)``               | Invoke update with job object and check status for any errors or warnings          |
+----------------------------------+------------------------------------------------------------------------------------+
| ``deleteJob(jid)``               | Delete job                                                                         |
+----------------------------------+------------------------------------------------------------------------------------+

Job Submission
==============

Job submission requires a job id. On successful submission, getStatus() method returns "BOOTING" or "RUNNING".

::

  //Job submission start
  MSubmission submission = client.startSubmission(1);
  System.out.println("Status : " + submission.getStatus());
  if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
    System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
  }
  System.out.println("Hadoop job id :" + submission.getExternalId());
  System.out.println("Job link : " + submission.getExternalLink());
  Counters counters = submission.getCounters();
  if(counters != null) {
    System.out.println("Counters:");
    for(CounterGroup group : counters) {
      System.out.print("\t");
      System.out.println(group.getName());
      for(Counter counter : group) {
        System.out.print("\t\t");
        System.out.print(counter.getName());
        System.out.print(": ");
        System.out.println(counter.getValue());
      }
    }
  }
  if(submission.getExceptionInfo() != null) {
    System.out.println("Exception info : " +submission.getExceptionInfo());
  }


  //Check job status
  MSubmission submission = client.getSubmissionStatus(1);
  if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
    System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
  }

  //Stop a running job
  submission.stopSubmission(jid);

Describe Forms
==========================

You can view the connection or job forms input values with labels of built-in resource bundle.

::

  String url = "http://localhost:12000/sqoop/";
  SqoopClient client = new SqoopClient(url);
  //Use getJob(jid) for describing job.
  //While printing connection forms, pass connector id to getResourceBundle(cid).
  describe(client.getConnection(1).getConnectorPart().getForms(), client.getResourceBundle(1));
  describe(client.getConnection(1).getFrameworkPart().getForms(), client.getFrameworkResourceBundle());

  void describe(List<MForm> forms, ResourceBundle resource) {
    for (MForm mf : forms) {
      System.out.println(resource.getString(mf.getLabelKey())+":");
      List<MInput<?>> mis = mf.getInputs();
      for (MInput mi : mis) {
        System.out.println(resource.getString(mi.getLabelKey()) + " : " + mi.getValue());
      }
      System.out.println();
    }
  }


Above Sqoop 2 Client API tutorial explained you how to create connection, create job and submit job.
