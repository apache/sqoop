/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.tools.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.json.ConnectionBean;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.json.SubmissionBean;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.tools.ConfiguredTool;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Load user-created content of Sqoop repository from a JSON formatted file
 * The loaded connector IDs will be modified to match existing connectors
 */
public class RepositoryLoadTool extends ConfiguredTool {

  public static final Logger LOG = Logger.getLogger(RepositoryLoadTool.class);



  @Override
  public boolean runToolWithConfiguration(String[] arguments) {


    Options options = new Options();
    options.addOption(OptionBuilder.isRequired()
            .hasArg()
            .withArgName("filename")
            .withLongOpt("input")
            .create('i'));

    CommandLineParser parser = new GnuParser();

    try {
      CommandLine line = parser.parse(options, arguments);
      String inputFileName = line.getOptionValue('i');

      LOG.info("Reading JSON from file" + inputFileName);
      InputStream input = new FileInputStream(inputFileName);
      String jsonTxt = IOUtils.toString(input, Charsets.UTF_8);
      JSONObject json =
              (JSONObject) JSONValue.parse(jsonTxt);
      boolean res = load(json);
      input.close();
      return res;

    } catch (FileNotFoundException e) {
      LOG.error("Repository dump file not found:", e);
      System.out.println("Input file not found. Please check Server logs for details.");
      return false;
    } catch (IOException e) {
      LOG.error("Unable to read repository dump file:", e);
      System.out.println("Unable to read input file. Please check Server logs for details.");
      return false;
    } catch (ParseException e) {
      LOG.error("Error parsing command line arguments:", e);
      System.out.println("Error parsing command line arguments. Please check Server logs for details.");
      return false;
    }
  }


  private boolean load(JSONObject repo) {

   // Validate that loading JSON into repository is supported
   JSONObject metadata = (JSONObject) repo.get(JSONConstants.METADATA);

   if (metadata == null) {
     LOG.error("Malformed JSON. Key "+ JSONConstants.METADATA + " not found.");
     return false;
   }

   if (!validateMetadata(metadata)){
     LOG.error("Metadata of repository dump file failed validation (see error above for cause). Aborting repository load.");
     return false;
   }

   // initialize repository as mutable
   RepositoryManager.getInstance().initialize(false);
   Repository repository = RepositoryManager.getInstance().getRepository();

   ConnectorManager.getInstance().initialize();
   ConnectorManager connectorManager = ConnectorManager.getInstance();

   FrameworkManager.getInstance().initialize();
   FrameworkManager frameworkManager = FrameworkManager.getInstance();

   LOG.info("Loading Connections");

   JSONObject jsonConns = (JSONObject) repo.get(JSONConstants.CONNECTIONS);

   if (jsonConns == null) {
     LOG.error("Malformed JSON file. Key "+ JSONConstants.CONNECTIONS + " not found.");
     return false;
   }

   ConnectionBean connectionBean = new ConnectionBean();
   connectionBean.restore(updateConnectorIDUsingName(jsonConns));

   HashMap<Long,Long> connectionIds = new HashMap<Long, Long>();

   for (MConnection connection : connectionBean.getConnections()) {
     long oldId = connection.getPersistenceId();
     long newId = loadConnection(connection);
     if (newId == connection.PERSISTANCE_ID_DEFAULT) {
       LOG.error("loading connection " + connection.getName() + " with previous ID " + oldId + " failed. Aborting repository load. Check log for details.");
       return false;
     }
     connectionIds.put(oldId,newId);
   }
   LOG.info("Loaded " + connectionIds.size() + " connections");

   LOG.info("Loading Jobs");
   JSONObject jsonJobs = (JSONObject) repo.get(JSONConstants.JOBS);

   if (jsonJobs == null) {
     LOG.error("Malformed JSON file. Key "+ JSONConstants.JOBS + " not found.");
     return false;
   }

   JobBean jobBean = new JobBean();
   jobBean.restore(updateIdUsingMap(updateConnectorIDUsingName(jsonJobs), connectionIds,JSONConstants.CONNECTION_ID));

   HashMap<Long,Long> jobIds = new HashMap<Long, Long>();
   for (MJob job: jobBean.getJobs()) {
     long oldId = job.getPersistenceId();
     long newId = loadJob(job);

     if (newId == job.PERSISTANCE_ID_DEFAULT) {
       LOG.error("loading job " + job.getName() + " failed. Aborting repository load. Check log for details.");
       return false;
     }
     jobIds.put(oldId,newId);

   }
    LOG.info("Loaded " + jobIds.size() + " jobs");

   LOG.info("Loading Submissions");
   JSONObject jsonSubmissions = (JSONObject) repo.get(JSONConstants.SUBMISSIONS);

    if (jsonSubmissions == null) {
      LOG.error("Malformed JSON file. Key "+ JSONConstants.SUBMISSIONS + " not found.");
      return false;
    }

   SubmissionBean submissionBean = new SubmissionBean();
   submissionBean.restore(updateIdUsingMap(jsonSubmissions,jobIds,JSONConstants.JOB_ID));
   int submissionCount = 0;
   for (MSubmission submission: submissionBean.getSubmissions()) {
     resetPersistenceId(submission);
     repository.createSubmission(submission);
     submissionCount++;
   }
    LOG.info("Loaded " + submissionCount + " submissions.");
    LOG.info("Repository load completed successfully.");
    return true;
  }

  private void resetPersistenceId(MPersistableEntity ent) {
    ent.setPersistenceId(ent.PERSISTANCE_ID_DEFAULT);
  }



  /**
   * Even though the metadata contains version, revision, compile-date and compile-user
   * We are only validating that version match for now.
   * More interesting logic can be added later
   */
  private boolean validateMetadata(JSONObject metadata) {
    String jsonVersion = (String) metadata.get(JSONConstants.VERSION);
    Boolean includeSensitive = (Boolean) metadata.get(JSONConstants.INCLUDE_SENSITIVE);
    String repoVersion = VersionInfo.getVersion();

    if (!jsonVersion.equals(repoVersion)) {
      LOG.error("Repository version in file (" + jsonVersion + ") does not match this version of Sqoop (" + repoVersion + ")");
      return false;
    }

    if (!includeSensitive) {
      LOG.warn("Loading repository which was dumped without --include-sensitive=true. " +
              "This means some sensitive information such as passwords is not included in the dump file and will need to be manually added later.");
    }

    return true;
  }

  private long loadConnection(MConnection connection) {

    //starting by pretending we have a brand new connection
    resetPersistenceId(connection);

    MetadataUpgrader upgrader = FrameworkManager.getInstance().getMetadataUpgrader();
    MFramework framework = FrameworkManager.getInstance().getFramework();
    Repository repository = RepositoryManager.getInstance().getRepository();

    List<MForm> frameworkForms = framework.getConnectionForms().clone(false).getForms();
    MConnectionForms newConnectionFrameworkForms = new MConnectionForms(frameworkForms);

    MConnector mConnector = ConnectorManager.getInstance().getConnectorMetadata(connection.getConnectorId());
    List<MForm> connectorForms = mConnector.getConnectionForms().clone(false).getForms();
    MConnectionForms newConnectionConnectorForms = new MConnectionForms(connectorForms);

    // upgrading the forms to make sure they match the current repository
    upgrader.upgrade(connection.getFrameworkPart(), newConnectionFrameworkForms);
    upgrader.upgrade(connection.getConnectorPart(), newConnectionConnectorForms);
    MConnection newConnection = new MConnection(connection, newConnectionConnectorForms, newConnectionFrameworkForms);

    // Transform form structures to objects for validations
    SqoopConnector connector =
            ConnectorManager.getInstance().getConnector(connection.getConnectorId());

    Object connectorConfig = ClassUtils.instantiate(
            connector.getConnectionConfigurationClass());
    Object frameworkConfig = ClassUtils.instantiate(
            FrameworkManager.getInstance().getConnectionConfigurationClass());

    FormUtils.fromForms(
            connection.getConnectorPart().getForms(), connectorConfig);
    FormUtils.fromForms(
            connection.getFrameworkPart().getForms(), frameworkConfig);

    Validator connectorValidator = connector.getValidator();
    Validator frameworkValidator = FrameworkManager.getInstance().getValidator();

    Validation connectorValidation =
            connectorValidator.validateConnection(connectorConfig);
    Validation frameworkValidation =
            frameworkValidator.validateConnection(frameworkConfig);

    Status finalStatus = Status.getWorstStatus(connectorValidation.getStatus(),
            frameworkValidation.getStatus());

    if (finalStatus.canProceed()) {
      repository.createConnection(newConnection);

    } else {
      LOG.error("Failed to load connection:" + connection.getName());
      LOG.error("Status of connector forms:" + connectorValidation.getStatus().toString());
      LOG.error("Status of framework forms:" + frameworkValidation.getStatus().toString());
    }
    return newConnection.getPersistenceId();
  }

  private long loadJob(MJob job) {
    //starting by pretending we have a brand new job
    resetPersistenceId(job);


    MetadataUpgrader upgrader = FrameworkManager.getInstance().getMetadataUpgrader();
    MFramework framework = FrameworkManager.getInstance().getFramework();
    Repository repository = RepositoryManager.getInstance().getRepository();

    MJob.Type jobType = job.getType();
    List<MForm> frameworkForms = framework.getJobForms(job.getType()).clone(false).getForms();
    MJobForms newJobFrameworkForms = new MJobForms(jobType,frameworkForms);

    MConnector mConnector = ConnectorManager.getInstance().getConnectorMetadata(job.getConnectorId());
    List<MForm> connectorForms = mConnector.getJobForms(jobType).clone(false).getForms();
    MJobForms newJobConnectorForms = new MJobForms(jobType,connectorForms);

    // upgrading the forms to make sure they match the current repository
    upgrader.upgrade(job.getFrameworkPart(), newJobFrameworkForms);
    upgrader.upgrade(job.getConnectorPart(), newJobConnectorForms);
    MJob newJob = new MJob(job, newJobConnectorForms, newJobFrameworkForms);

    // Transform form structures to objects for validations
    SqoopConnector connector =
            ConnectorManager.getInstance().getConnector(job.getConnectorId());

    Object connectorConfig = ClassUtils.instantiate(
            connector.getJobConfigurationClass(jobType));
    Object frameworkConfig = ClassUtils.instantiate(
            FrameworkManager.getInstance().getJobConfigurationClass(jobType));

    FormUtils.fromForms(
            job.getConnectorPart().getForms(), connectorConfig);
    FormUtils.fromForms(
            job.getFrameworkPart().getForms(), frameworkConfig);

    Validator connectorValidator = connector.getValidator();
    Validator frameworkValidator = FrameworkManager.getInstance().getValidator();

    Validation connectorValidation =
            connectorValidator.validateJob(jobType,connectorConfig);
    Validation frameworkValidation =
            frameworkValidator.validateJob(jobType,frameworkConfig);

    Status finalStatus = Status.getWorstStatus(connectorValidation.getStatus(),
            frameworkValidation.getStatus());

    if (finalStatus.canProceed()) {
      repository.createJob(newJob);

    } else {
      LOG.error("Failed to load job:" + job.getName());
      LOG.error("Status of connector forms:" + connectorValidation.getStatus().toString());
      LOG.error("Status of framework forms:" + frameworkValidation.getStatus().toString());

    }
    return newJob.getPersistenceId();


  }

  private JSONObject updateConnectorIDUsingName( JSONObject json) {
    JSONArray array = (JSONArray) json.get(JSONConstants.ALL);

    Repository repository = RepositoryManager.getInstance().getRepository();

    List<MConnector> connectors = repository.findConnectors();
    Map<String, Long> connectorMap = new HashMap<String, Long>();

    for (MConnector connector : connectors) {
      connectorMap.put(connector.getUniqueName(), connector.getPersistenceId());
    }

    for (Object obj : array) {
      JSONObject object = (JSONObject) obj;
      long connectorId = (Long) object.get(JSONConstants.CONNECTOR_ID);
      String connectorName = (String) object.get(JSONConstants.CONNECTOR_NAME);
      long currentConnectorId = connectorMap.get(connectorName);
      String connectionName = (String) object.get(JSONConstants.NAME);


      // If a given connector now has a different ID, we need to update the ID
      if (connectorId != currentConnectorId) {
        LOG.warn("Connection " + connectionName + " uses connector " + connectorName + ". " +
                "Replacing previous ID " + connectorId + " with new ID " + currentConnectorId);

        object.put(JSONConstants.CONNECTOR_ID, currentConnectorId);
      }
    }
    return json;
  }

  private JSONObject updateIdUsingMap(JSONObject json, HashMap<Long,Long> idMap, String fieldName) {
    JSONArray array = (JSONArray) json.get(JSONConstants.ALL);

    for (Object obj : array) {
      JSONObject object = (JSONObject) obj;

      object.put(fieldName, idMap.get(object.get(fieldName)));
    }

    return json;
  }



}
