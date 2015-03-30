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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.driver.DriverUpgrader;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.json.JobsBean;
import org.apache.sqoop.json.LinksBean;
import org.apache.sqoop.json.SubmissionsBean;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.tools.ConfiguredTool;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.ConfigValidationRunner;
import org.apache.sqoop.validation.Status;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Load user-created content of Sqoop repository from a JSON formatted file The
 * loaded connector IDs will be modified to match existing connectors
 */
public class RepositoryLoadTool extends ConfiguredTool {

  public static final Logger LOG = Logger.getLogger(RepositoryLoadTool.class);

  @SuppressWarnings("static-access")
  @Override
  public boolean runToolWithConfiguration(String[] arguments) {

    Options options = new Options();
    options.addOption(OptionBuilder.isRequired().hasArg().withArgName("filename")
        .withLongOpt("input").create('i'));

    CommandLineParser parser = new GnuParser();

    try {
      CommandLine line = parser.parse(options, arguments);
      String inputFileName = line.getOptionValue('i');

      LOG.info("Reading JSON from file" + inputFileName);
      InputStream input = new FileInputStream(inputFileName);
      String jsonTxt = IOUtils.toString(input, Charsets.UTF_8);
      JSONObject json = JSONUtils.parse(jsonTxt);
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
      System.out
          .println("Error parsing command line arguments. Please check Server logs for details.");
      return false;
    }
  }

  private boolean load(JSONObject repo) {

    // Validate that loading JSON into repository is supported
    JSONObject metadata = (JSONObject) repo.get(JSONConstants.METADATA);

    if (metadata == null) {
      LOG.error("Malformed JSON. Key " + JSONConstants.METADATA + " not found.");
      return false;
    }

    if (!validateMetadata(metadata)) {
      LOG.error("Metadata of repository dump file failed validation (see error above for cause). Aborting repository load.");
      return false;
    }

    // initialize repository as mutable
    RepositoryManager.getInstance().initialize(false);
    Repository repository = RepositoryManager.getInstance().getRepository();

    ConnectorManager.getInstance().initialize();
    LOG.info("Loading Connections");

    JSONObject jsonLinks = (JSONObject) repo.get(JSONConstants.LINKS);

    if (jsonLinks == null) {
      LOG.error("Malformed JSON file. Key " + JSONConstants.LINKS + " not found.");
      return false;
    }

    updateConnectorIDUsingName(
        (JSONArray)jsonLinks.get(JSONConstants.LINKS),
        JSONConstants.CONNECTOR_ID);

    LinksBean linksBean = new LinksBean();
    linksBean.restore(jsonLinks);

    HashMap<Long, Long> linkIds = new HashMap<Long, Long>();

    for (MLink link : linksBean.getLinks()) {
      long oldId = link.getPersistenceId();
      long newId = loadLink(link);
      if (newId == link.PERSISTANCE_ID_DEFAULT) {
        LOG.error("loading connection " + link.getName() + " with previous ID " + oldId
            + " failed. Aborting repository load. Check log for details.");
        return false;
      }
      linkIds.put(oldId, newId);
    }
    LOG.info("Loaded " + linkIds.size() + " links");

    LOG.info("Loading Jobs");
    JSONObject jsonJobs = (JSONObject) repo.get(JSONConstants.JOBS);

    if (jsonJobs == null) {
      LOG.error("Malformed JSON file. Key " + JSONConstants.JOBS + " not found.");
      return false;
    }

    updateConnectorIDUsingName(
        (JSONArray)jsonJobs.get(JSONConstants.JOBS),
        JSONConstants.FROM_CONNECTOR_ID);
    updateConnectorIDUsingName(
        (JSONArray)jsonJobs.get(JSONConstants.JOBS),
        JSONConstants.TO_CONNECTOR_ID);
    updateIdUsingMap((JSONArray)jsonJobs.get(JSONConstants.JOBS),
        linkIds, JSONConstants.LINK_ID);

    JobsBean jobsBean = new JobsBean();
    jobsBean.restore(jsonJobs);

    HashMap<Long, Long> jobIds = new HashMap<Long, Long>();
    for (MJob job : jobsBean.getJobs()) {
      long oldId = job.getPersistenceId();
      long newId = loadJob(job);

      if (newId == job.PERSISTANCE_ID_DEFAULT) {
        LOG.error("loading job " + job.getName()
            + " failed. Aborting repository load. Check log for details.");
        return false;
      }
      jobIds.put(oldId, newId);

    }
    LOG.info("Loaded " + jobIds.size() + " jobs");

    LOG.info("Loading Submissions");
    JSONObject jsonSubmissions = (JSONObject) repo.get(JSONConstants.SUBMISSIONS);

    if (jsonSubmissions == null) {
      LOG.error("Malformed JSON file. Key " + JSONConstants.SUBMISSIONS + " not found.");
      return false;
    }

    updateIdUsingMap((JSONArray)jsonSubmissions.get(JSONConstants.SUBMISSIONS), jobIds, JSONConstants.JOB_ID);

    SubmissionsBean submissionsBean = new SubmissionsBean();
    submissionsBean.restore(jsonSubmissions);
    int submissionCount = 0;
    for (MSubmission submission : submissionsBean.getSubmissions()) {
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
   * Even though the metadata contains version, revision, compile-date and
   * compile-user We are only validating that version match for now. More
   * interesting logic can be added later
   */
  private boolean validateMetadata(JSONObject metadata) {
    String jsonVersion = (String) metadata.get(JSONConstants.VERSION);
    Boolean includeSensitive = (Boolean) metadata.get(JSONConstants.INCLUDE_SENSITIVE);
    String repoVersion = VersionInfo.getBuildVersion();

    if (!jsonVersion.equals(repoVersion)) {
      LOG.error("Repository version in file (" + jsonVersion
          + ") does not match this version of Sqoop (" + repoVersion + ")");
      return false;
    }

    if (!includeSensitive) {
      LOG.warn("Loading repository which was dumped without --include-sensitive=true. "
          + "This means some sensitive information such as passwords is not included in the dump file and will need to be manually added later.");
    }

    return true;
  }

  private long loadLink(MLink link) {

    // starting by pretending we have a brand new link
    resetPersistenceId(link);

    Repository repository = RepositoryManager.getInstance().getRepository();

    MConnector mConnector = ConnectorManager.getInstance().getConnectorConfigurable(link.getConnectorId());
    ConnectorConfigurableUpgrader connectorConfigUpgrader = ConnectorManager.getInstance().getSqoopConnector(mConnector.getUniqueName()).getConfigurableUpgrader();

    List<MConfig> connectorConfigs = mConnector.getLinkConfig().clone(false).getConfigs();
    MLinkConfig newLinkConfigs = new MLinkConfig(connectorConfigs);

    // upgrading the configs to make sure they match the current repository
    connectorConfigUpgrader.upgradeLinkConfig(link.getConnectorLinkConfig(), newLinkConfigs);
    MLink newLink = new MLink(link, newLinkConfigs);

    // Transform config structures to objects for validations
    SqoopConnector connector = ConnectorManager.getInstance().getSqoopConnector(
        link.getConnectorId());

    Object connectorConfig = ClassUtils.instantiate(connector.getLinkConfigurationClass());

    ConfigUtils.fromConfigs(link.getConnectorLinkConfig().getConfigs(), connectorConfig);

    ConfigValidationRunner validationRunner = new ConfigValidationRunner();
    ConfigValidationResult result = validationRunner.validate(connectorConfig);

    Status finalStatus = Status.getWorstStatus(result.getStatus());

    if (finalStatus.canProceed()) {
      repository.createLink(newLink);

    } else {
      LOG.error("Failed to load link:" + link.getName());
      LOG.error("Status of connector configs:" + result.getStatus().toString());
    }
    return newLink.getPersistenceId();
  }

  private long loadJob(MJob job) {
    // starting by pretending we have a brand new job
    resetPersistenceId(job);
    MConnector mFromConnector = ConnectorManager.getInstance().getConnectorConfigurable(job.getFromConnectorId());
    MConnector mToConnector = ConnectorManager.getInstance().getConnectorConfigurable(job.getToConnectorId());

    MFromConfig fromConfig = job.getFromJobConfig();
    MToConfig toConfig = job.getToJobConfig();

    ConnectorConfigurableUpgrader fromConnectorConfigUpgrader = ConnectorManager.getInstance().getSqoopConnector(mFromConnector.getUniqueName()).getConfigurableUpgrader();
    ConnectorConfigurableUpgrader toConnectorConfigUpgrader = ConnectorManager.getInstance().getSqoopConnector(mToConnector.getUniqueName()).getConfigurableUpgrader();

    fromConnectorConfigUpgrader.upgradeFromJobConfig(job.getFromJobConfig(), fromConfig);

    toConnectorConfigUpgrader.upgradeToJobConfig(job.getToJobConfig(), toConfig);

    DriverUpgrader driverConfigUpgrader =  Driver.getInstance().getConfigurableUpgrader();
    MDriver driver = Driver.getInstance().getDriver();
    MDriverConfig driverConfigs = driver.getDriverConfig();
    driverConfigUpgrader.upgradeJobConfig( job.getDriverConfig(), driverConfigs);

    MJob newJob = new MJob(job, fromConfig, toConfig, driverConfigs);

    // Transform config structures to objects for validations
    SqoopConnector fromConnector =
        ConnectorManager.getInstance().getSqoopConnector(
            job.getFromConnectorId());
    SqoopConnector toConnector =
        ConnectorManager.getInstance().getSqoopConnector(
            job.getToConnectorId());

    Object fromConnectorConfig = ClassUtils.instantiate(
        fromConnector.getJobConfigurationClass(Direction.FROM));
    Object toConnectorConfig = ClassUtils.instantiate(
        toConnector.getJobConfigurationClass(Direction.TO));
    Object driverConfig = ClassUtils.instantiate(
            Driver.getInstance().getDriverJobConfigurationClass());

    ConfigUtils.fromConfigs(
        job.getFromJobConfig().getConfigs(), fromConnectorConfig);
    ConfigUtils.fromConfigs(
        job.getToJobConfig().getConfigs(), toConnectorConfig);
    ConfigUtils.fromConfigs(
        job.getDriverConfig().getConfigs(), driverConfig);

    ConfigValidationRunner validationRunner = new ConfigValidationRunner();
    ConfigValidationResult fromConnectorConfigResult = validationRunner
        .validate(fromConnectorConfig);
    ConfigValidationResult toConnectorConfigResult = validationRunner.validate(toConnectorConfig);
    ConfigValidationResult driverConfigResult = validationRunner.validate(driverConfig);

    Status finalStatus = Status.getWorstStatus(fromConnectorConfigResult.getStatus(),
        toConnectorConfigResult.getStatus(), driverConfigResult.getStatus());

    if (finalStatus.canProceed()) {
      RepositoryManager.getInstance().getRepository().createJob(newJob);

    } else {
      LOG.error("Failed to load job:" + job.getName());
      LOG.error("Status of from connector configs:"
          + fromConnectorConfigResult.getStatus().toString());
      LOG.error("Status of to connector configs:" + toConnectorConfigResult.getStatus().toString());
      LOG.error("Status of driver configs:" + driverConfigResult.getStatus().toString());

    }
    return newJob.getPersistenceId();

  }

  private JSONArray updateConnectorIDUsingName(JSONArray jsonArray, String connectorIdKey) {
    Repository repository = RepositoryManager.getInstance().getRepository();

    List<MConnector> connectors = repository.findConnectors();
    Map<String, Long> connectorMap = new HashMap<String, Long>();

    for (MConnector connector : connectors) {
      connectorMap.put(connector.getUniqueName(), connector.getPersistenceId());
    }

    for (Object obj : jsonArray) {
      JSONObject object = (JSONObject) obj;
      long connectorId = (Long) object.get(connectorIdKey);
      String connectorName = (String) object.get(JSONConstants.CONNECTOR_NAME);
      long currentConnectorId = connectorMap.get(connectorName);
      String linkName = (String) object.get(JSONConstants.NAME);

      // If a given connector now has a different ID, we need to update the ID
      if (connectorId != currentConnectorId) {
        LOG.warn("Link " + linkName + " uses connector " + connectorName + ". "
            + "Replacing previous ID " + connectorId + " with new ID " + currentConnectorId);

        object.put(connectorIdKey, currentConnectorId);
      }
    }

    return jsonArray;
  }

  private JSONArray updateIdUsingMap(JSONArray jsonArray, HashMap<Long, Long> idMap, String fieldName) {
    for (Object obj : jsonArray) {
      JSONObject object = (JSONObject) obj;

      object.put(fieldName, idMap.get(object.get(fieldName)));
    }

    return jsonArray;
  }
}
