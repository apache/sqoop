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


===========================
Sqoop Java Client API Guide
===========================

This document will explain how to use Sqoop Java Client API with external application. Client API allows you to execute the functions of sqoop commands. It requires Sqoop Client JAR and its dependencies.

The main class that provides wrapper methods for all the supported operations is the
::

  public class SqoopClient {
    ...
  }

Java Client API is explained using Generic JDBC Connector example. Before executing the application using the sqoop client API, check whether sqoop server is running.

Workflow
========

Given workflow has to be followed for executing a sqoop job in Sqoop server.

  1. Create LINK object for a given connector name              - Creates Link object and returns it
  2. Create a JOB for a given "from" and "to" link name         - Create Job object and returns it
  3. Start the JOB for a given job name                         - Start Job on the server and creates a submission record

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


Link
====
Connectors provide the facility to interact with many data sources and thus can be used as a means to transfer data between them in Sqoop. The registered connector implementation will provide logic to read from and/or write to a data source that it represents. A connector can have one or more links associated with it. The java client API allows you to create, update and delete a link for any registered connector. Creating or updating a link requires you to populate the Link Config for that particular connector. Hence the first thing to do is get the list of registered connectors and select the connector for which you would like to create a link. Then
you can get the list of all the config/inputs using `Display Config and Input Names For Connector`_ for that connector.


Save Link
---------

First create a new link by invoking ``createLink(connectorName)`` method with connector name and it returns a MLink object with dummy id and the unfilled link config inputs for that connector. Then fill the config inputs with relevant values. Invoke ``saveLink`` passing it the filled MLink object.

::

  // create a placeholder for link
  MLink link = client.createLink("connectorName");
  link.setName("Vampire");
  link.setCreationUser("Buffy");
  MLinkConfig linkConfig = link.getConnectorLinkConfig();
  // fill in the link config values
  linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://localhost/my");
  linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
  linkConfig.getStringInput("linkConfig.username").setValue("root");
  linkConfig.getStringInput("linkConfig.password").setValue("root");
  // save the link object that was filled
  Status status = client.saveLink(link);
  if(status.canProceed()) {
   System.out.println("Created Link with Link Name : " + link.getName());
  } else {
   System.out.println("Something went wrong creating the link");
  }

``status.canProceed()`` returns true if status is OK or a WARNING. Before sending the status, the link config values are validated using the corresponding validator associated with th link config inputs.

On successful execution of the saveLink method, new link name is assigned to the link object else an exception is thrown. ``link.getName()`` method returns the unique name for this object persisted in the sqoop repository.

User can retrieve a link using the following methods

+----------------------------+--------------------------------------+
|   Method                   | Description                          |
+============================+======================================+
| ``getLink(linkName)``      | Returns a link by name               |
+----------------------------+--------------------------------------+
| ``getLinks()``             | Returns list of links in the sqoop   |
+----------------------------+--------------------------------------+

Job
===

A sqoop job holds the ``From`` and ``To`` parts for transferring data from the ``From`` data source to the ``To`` data source. Both the ``From`` and the ``To`` are uniquely identified by their corresponding connector Link Ids. i.e when creating a job we have to specifiy the ``FromLinkId`` and the ``ToLinkId``. Thus the pre-requisite for creating a job is to first create the links as described above.

Once the link names for the ``From`` and ``To`` are given, then the job configs for the associated connector for the link object have to be filled. You can get the list of all the from and to job config/inputs using `Display Config and Input Names For Connector`_ for that connector. A connector can have one or more links. We then use the links in the ``From`` and ``To`` direction to populate the corresponding ``MFromConfig`` and ``MToConfig`` respectively.

In addition to filling the job configs for the ``From`` and the ``To`` representing the link, we also need to fill the driver configs that control the job execution engine environment. For example, if the job execution engine happens to be the MapReduce we will specifiy the number of mappers to be used in reading data from the ``From`` data source.

Save Job
---------
Here is the code to create and then save a job
::

  String url = "http://localhost:12000/sqoop/";
  SqoopClient client = new SqoopClient(url);
  //Creating dummy job object
  MJob job = client.createJob("fromLinkName", "toLinkName");
  job.setName("Vampire");
  job.setCreationUser("Buffy");
  // set the "FROM" link job config values
  MFromConfig fromJobConfig = job.getFromJobConfig();
  fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("sqoop");
  fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("sqoop");
  fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");
  // set the "TO" link job config values
  MToConfig toJobConfig = job.getToJobConfig();
  toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/usr/tmp");
  // set the driver config values
  MDriverConfig driverConfig = job.getDriverConfig();
  driverConfig.getStringInput("throttlingConfig.numExtractors").setValue("3");

  Status status = client.saveJob(job);
  if(status.canProceed()) {
   System.out.println("Created Job with Job Name: "+ job.getName());
  } else {
   System.out.println("Something went wrong creating the job");
  }

User can retrieve a job using the following methods

+----------------------------+--------------------------------------+
|   Method                   | Description                          |
+============================+======================================+
| ``getJob(jobName)``        | Returns a job by name                |
+----------------------------+--------------------------------------+
| ``getJobs()``              | Returns list of jobs in the sqoop    |
+----------------------------+--------------------------------------+


List of status codes
--------------------

+------------------+------------------------------------------------------------------------------------------------------------+
| Function         | Description                                                                                                |
+==================+============================================================================================================+
| ``OK``           | There are no issues, no warnings.                                                                          |
+------------------+------------------------------------------------------------------------------------------------------------+
| ``WARNING``      | Validated entity is correct enough to be proceed. Not a fatal error                                        |
+------------------+------------------------------------------------------------------------------------------------------------+
| ``ERROR``        | There are serious issues with validated entity. We can't proceed until reported issues will be resolved.   |
+------------------+------------------------------------------------------------------------------------------------------------+

View Error or Warning valdiation message
----------------------------------------

In case of any WARNING AND ERROR status, user has to iterate the list of validation messages.

::

 printMessage(link.getConnectorLinkConfig().getConfigs());

 private static void printMessage(List<MConfig> configs) {
   for(MConfig config : configs) {
     List<MInput<?>> inputlist = config.getInputs();
     if (config.getValidationMessages() != null) {
      // print every validation message
      for(Message message : config.getValidationMessages()) {
       System.out.println("Config validation message: " + message.getMessage());
      }
     }
     for (MInput minput : inputlist) {
       if (minput.getValidationStatus() == Status.WARNING) {
        for(Message message : minput.getValidationMessages()) {
         System.out.println("Config Input Validation Warning: " + message.getMessage());
       }
     }
     else if (minput.getValidationStatus() == Status.ERROR) {
       for(Message message : minput.getValidationMessages()) {
        System.out.println("Config Input Validation Error: " + message.getMessage());
       }
      }
     }
    }

Updating link and job
---------------------
After creating link or job in the repository, you can update or delete a link or job using the following functions

+----------------------------------+------------------------------------------------------------------------------------+
|   Method                         | Description                                                                        |
+==================================+====================================================================================+
| ``updateLink(link)``             | Invoke update with link and check status for any errors or warnings                |
+----------------------------------+------------------------------------------------------------------------------------+
| ``deleteLink(linkName)``         | Delete link. Deletes only if specified link is not used by any job                 |
+----------------------------------+------------------------------------------------------------------------------------+
| ``updateJob(job)``               | Invoke update with job and check status for any errors or warnings                 |
+----------------------------------+------------------------------------------------------------------------------------+
| ``deleteJob(jobName)``           | Delete job                                                                         |
+----------------------------------+------------------------------------------------------------------------------------+

Job Start
==============

Starting a job requires a job name. On successful start, getStatus() method returns "BOOTING" or "RUNNING".

::

  //Job start
  MSubmission submission = client.startJob("jobName");
  System.out.println("Job Submission Status : " + submission.getStatus());
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


  //Check job status for a running job
  MSubmission submission = client.getJobStatus("jobName");
  if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
    System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
  }

  //Stop a running job
  submission.stopJob("jobName");

Above code block, job start is asynchronous. For synchronous job start, use ``startJob(jobName, callback, pollTime)`` method. If you are not interested in getting the job status, then invoke the same method with "null" as the value for the callback parameter and this returns the final job status. ``pollTime`` is the request interval for getting the job status from sqoop server and the value should be greater than zero. We will frequently hit the sqoop server if a low value is given for the ``pollTime``. When a synchronous job is started with a non null callback, it first invokes the callback's ``submitted(MSubmission)`` method on successful start, after every poll time interval, it then invokes the ``updated(MSubmission)`` method on the callback API and finally on finishing the job executuon it invokes the ``finished(MSubmission)`` method on the callback API.

Display Config and Input Names For Connector
============================================

You can view the config/input names for the link and job config types per connector

::

  String url = "http://localhost:12000/sqoop/";
  SqoopClient client = new SqoopClient(url);
  String connectorName = "connectorName";
  // link config for connector
  describe(client.getConnector(connectorName).getLinkConfig().getConfigs(), client.getConnectorConfigBundle(connectorName));
  // from job config for connector
  describe(client.getConnector(connectorName).getFromConfig().getConfigs(), client.getConnectorConfigBundle(connectorName));
  // to job config for the connector
  describe(client.getConnector(connectorName).getToConfig().getConfigs(), client.getConnectorConfigBundle(connectorName));

  void describe(List<MConfig> configs, ResourceBundle resource) {
    for (MConfig config : configs) {
      System.out.println(resource.getString(config.getLabelKey())+":");
      List<MInput<?>> inputs = config.getInputs();
      for (MInput input : inputs) {
        System.out.println(resource.getString(input.getLabelKey()) + " : " + input.getValue());
      }
      System.out.println();
    }
  }


Above Sqoop 2 Client API tutorial explained how to create a link, create job and and then start the job.
