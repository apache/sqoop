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
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.cli.SqoopGnuParser;
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
import org.apache.sqoop.model.MValidator;
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
  private boolean isInTest = false;

  @SuppressWarnings("static-access")
  @Override
  public boolean runToolWithConfiguration(String[] arguments) {

    Options options = new Options();
    options.addOption(OptionBuilder.isRequired().hasArg().withArgName("filename")
        .withLongOpt("input").create('i'));

    CommandLineParser parser = new SqoopGnuParser();

    try {
      CommandLine line = parser.parse(options, arguments);
      String inputFileName = line.getOptionValue('i');

      LOG.info("Reading JSON from file" + inputFileName);
      try (InputStream input = new FileInputStream(inputFileName)) {
        String jsonTxt = IOUtils.toString(input, Charsets.UTF_8);
        JSONObject json = JSONUtils.parse(jsonTxt);
        boolean res = load(json);
        return res;
      }
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

    if (!isInTest) {
      // initialize repository as mutable
      RepositoryManager.getInstance().initialize(false);
      ConnectorManager.getInstance().initialize();
      Driver.getInstance().initialize();
    }

    Repository repository = RepositoryManager.getInstance().getRepository();

    LOG.info("Loading Connections");
    JSONObject jsonLinks = (JSONObject) repo.get(JSONConstants.LINKS);

    if (jsonLinks == null) {
      LOG.error("Malformed JSON file. Key " + JSONConstants.LINKS + " not found.");
      return false;
    }

    removeObjectIfConnectorNotExist(
            (JSONArray) jsonLinks.get(JSONConstants.LINKS),
            JSONConstants.CONNECTOR_NAME, true);

    LinksBean linksBean = new LinksBean();
    linksBean.restore(jsonLinks);

    for (MLink link : linksBean.getLinks()) {
      long newId = loadLink(link);
      if (newId == link.PERSISTANCE_ID_DEFAULT) {
        LOG.error("loading connection " + link.getName() + " failed. Aborting repository load. Check log for details.");
        return false;
      }
    }
    LOG.info("Loaded " + linksBean.getLinks().size() + " links");

    LOG.info("Loading Jobs");
    JSONObject jsonJobs = (JSONObject) repo.get(JSONConstants.JOBS);

    if (jsonJobs == null) {
      LOG.error("Malformed JSON file. Key " + JSONConstants.JOBS + " not found.");
      return false;
    }

    removeObjectIfConnectorNotExist(
            (JSONArray) jsonJobs.get(JSONConstants.JOBS),
            JSONConstants.FROM_CONNECTOR_NAME, false);
    removeObjectIfConnectorNotExist(
            (JSONArray) jsonJobs.get(JSONConstants.JOBS),
            JSONConstants.TO_CONNECTOR_NAME, false);

    removeJobIfLinkNotExist((JSONArray) jsonJobs.get(JSONConstants.JOBS),
            JSONConstants.FROM_LINK_NAME);
    removeJobIfLinkNotExist((JSONArray) jsonJobs.get(JSONConstants.JOBS),
            JSONConstants.TO_LINK_NAME);

    JobsBean jobsBean = new JobsBean();
    jobsBean.restore(jsonJobs);

    for (MJob job : jobsBean.getJobs()) {
      long newId = loadJob(job);

      if (newId == job.PERSISTANCE_ID_DEFAULT) {
        LOG.error("loading job " + job.getName()
            + " failed. Aborting repository load. Check log for details.");
        return false;
      }
    }
    LOG.info("Loaded " + jobsBean.getJobs().size() + " jobs");

    LOG.info("Loading Submissions");
    JSONObject jsonSubmissions = (JSONObject) repo.get(JSONConstants.SUBMISSIONS);

    if (jsonSubmissions == null) {
      LOG.error("Malformed JSON file. Key " + JSONConstants.SUBMISSIONS + " not found.");
      return false;
    }

    removeSubmissionIfJobNotExist((JSONArray)jsonSubmissions.get(JSONConstants.SUBMISSIONS));

    SubmissionsBean submissionsBean = new SubmissionsBean();
    submissionsBean.restore(jsonSubmissions);
    for (MSubmission submission : submissionsBean.getSubmissions()) {
      resetPersistenceId(submission);
      repository.createSubmission(submission);
    }
    LOG.info("Loaded " + submissionsBean.getSubmissions().size() + " submissions.");
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

  /**
   * We currently pass through null as the old connector version because we do
   * not have a good way of determining what the old version of the connector is
   * here.
   *
   * According to Jarcec, this chunk of code will receive some much needed
   * attention in the near future and this will be fixed.
   */
  private long loadLink(MLink link) {

    // starting by pretending we have a brand new link
    resetPersistenceId(link);

    Repository repository = RepositoryManager.getInstance().getRepository();

    MConnector mConnector = ConnectorManager.getInstance().getConnectorConfigurable(link.getConnectorName());
    ConnectorConfigurableUpgrader connectorConfigUpgrader = ConnectorManager.getInstance().getSqoopConnector(mConnector.getUniqueName()).getConfigurableUpgrader(null);

    List<MConfig> connectorConfigs = mConnector.getLinkConfig().clone(false).getConfigs();
    List<MValidator> connectorValidators = mConnector.getLinkConfig().getCloneOfValidators();
    MLinkConfig newLinkConfigs = new MLinkConfig(connectorConfigs, connectorValidators);

    // upgrading the configs to make sure they match the current repository
    connectorConfigUpgrader.upgradeLinkConfig(link.getConnectorLinkConfig(), newLinkConfigs);
    MLink newLink = new MLink(link, newLinkConfigs);

    // Transform config structures to objects for validations
    SqoopConnector connector = ConnectorManager.getInstance().getSqoopConnector(
        link.getConnectorName());

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


  /**
   * We currently pass through null as the old connector version because we do
   * not have a good way of determining what the old version of the connector is
   * here.
   *
   * According to Jarcec, this chunk of code will receive some much needed
   * attention in the near future and this will be fixed.
   */
  private long loadJob(MJob job) {
    // starting by pretending we have a brand new job
    resetPersistenceId(job);
    MConnector mFromConnector = ConnectorManager.getInstance().getConnectorConfigurable(job.getFromConnectorName());
    MConnector mToConnector = ConnectorManager.getInstance().getConnectorConfigurable(job.getToConnectorName());

    MFromConfig fromConfig = job.getFromJobConfig();
    MToConfig toConfig = job.getToJobConfig();

    ConnectorConfigurableUpgrader fromConnectorConfigUpgrader = ConnectorManager.getInstance().getSqoopConnector(mFromConnector.getUniqueName()).getConfigurableUpgrader(null);
    ConnectorConfigurableUpgrader toConnectorConfigUpgrader = ConnectorManager.getInstance().getSqoopConnector(mToConnector.getUniqueName()).getConfigurableUpgrader(null);

    fromConnectorConfigUpgrader.upgradeFromJobConfig(job.getFromJobConfig(), fromConfig);

    toConnectorConfigUpgrader.upgradeToJobConfig(job.getToJobConfig(), toConfig);

    DriverUpgrader driverConfigUpgrader =  Driver.getInstance().getConfigurableUpgrader(null);
    MDriver driver = Driver.getInstance().getDriver();
    MDriverConfig driverConfigs = driver.getDriverConfig();
    driverConfigUpgrader.upgradeJobConfig( job.getDriverConfig(), driverConfigs);

    MJob newJob = new MJob(job, fromConfig, toConfig, driverConfigs);

    // Transform config structures to objects for validations
    SqoopConnector fromConnector =
        ConnectorManager.getInstance().getSqoopConnector(
            job.getFromConnectorName());
    SqoopConnector toConnector =
        ConnectorManager.getInstance().getSqoopConnector(
            job.getToConnectorName());

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

  private JSONArray removeObjectIfConnectorNotExist(JSONArray jsonArray, String connectorNameKey, boolean isLink) {
    Repository repository = RepositoryManager.getInstance().getRepository();

    List<MConnector> connectors = repository.findConnectors();
    List<String> connectorNames = new ArrayList<String>();

    for (MConnector connector : connectors) {
      connectorNames.add(connector.getUniqueName());
    }

    for (Iterator iterator = jsonArray.iterator(); iterator.hasNext(); ) {
      JSONObject object = (JSONObject) iterator.next();
      String connectorName = (String) object.get(connectorNameKey);
      String objectName = (String)object.get(JSONConstants.NAME);

      if (!connectorNames.contains(connectorName)) {
        // If a connector doesn't exist, remove the links and jobs relating to it
        iterator.remove();
        LOG.warn((isLink ? "Link " : "Job ") + objectName + " won't be loaded because connector "
                + connectorName + " is missing.");
        continue;
      }
    }

    return jsonArray;
  }

  private JSONArray removeJobIfLinkNotExist(JSONArray jobsJsonArray, String linkNameKey) {
    Repository repository = RepositoryManager.getInstance().getRepository();

    List<MLink> links = repository.findLinks();
    List<String> linkNames = new ArrayList<String>();
    for (MLink link : links) {
      linkNames.add(link.getName());
    }

    for(Iterator iterator = jobsJsonArray.iterator(); iterator.hasNext(); ) {
      JSONObject jobObject = (JSONObject) iterator.next();
      String linkName = (String) jobObject.get(linkNameKey);
      String jobName = (String)jobObject.get(JSONConstants.NAME);

      if (!linkNames.contains(linkName)) {
        // If a link doesn't exist, remove the jobs relating to it
        iterator.remove();
        LOG.warn("Job " + jobName + " won't be loaded because link " + linkName + " is missing.");
        continue;
      }
    }

    return jobsJsonArray;
  }

  private JSONArray removeSubmissionIfJobNotExist(JSONArray submissionsJsonArray) {
    Repository repository = RepositoryManager.getInstance().getRepository();

    List<MJob> jobs = repository.findJobs();
    List<String> jobNames = new ArrayList<String>();
    for (MJob job : jobs) {
      jobNames.add(job.getName());
    }

    for(Iterator iterator = submissionsJsonArray.iterator(); iterator.hasNext(); ) {
      JSONObject submissionObject = (JSONObject) iterator.next();
      String jobName = (String) submissionObject.get(JSONConstants.JOB_NAME);

      if (!jobNames.contains(jobName)) {
        // If a job doesn't exist, remove the submissions relating to it
        iterator.remove();
        LOG.warn("Submission for " + jobName + " won't be loaded because job " + jobName + " is missing.");
        continue;
      }
    }

    return submissionsJsonArray;
  }

  public void setInTest(boolean isInTest) {
    this.isInTest = isInTest;
  }
}
