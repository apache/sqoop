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


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.json.JobsBean;
import org.apache.sqoop.json.LinksBean;
import org.apache.sqoop.json.SubmissionsBean;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.tools.ConfiguredTool;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Write user-created content of Sqoop repository to JSON formatted file
 */
public class RepositoryDumpTool extends ConfiguredTool {
  public static final Logger LOG = Logger.getLogger(RepositoryDumpTool.class);

  @Override
  public boolean runToolWithConfiguration(String[] arguments) {

    boolean skipSensitive = true;

    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt("include-sensitive")
            .withDescription("Dump all data including sensitive information such as passwords. Passwords will be dumped in clear text")
            .create());
    options.addOption(OptionBuilder.isRequired()
            .hasArg()
            .withArgName("filename")
            .withLongOpt("output")
            .create('o'));

    CommandLineParser parser = new GnuParser();

    try {
      CommandLine line = parser.parse(options, arguments);
      String outputFileName = line.getOptionValue('o');

      if (line.hasOption("include-sensitive")) {
        skipSensitive = false;
      }

      BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileName), "UTF-8"));
      LOG.info("Writing JSON repository dump to file " + outputFileName);
      dump(skipSensitive).writeJSONString(output);
      output.flush();
      output.close();

    } catch (ParseException e) {
      LOG.error("Error parsing command line arguments:", e);
      System.out.println("Error parsing command line arguments. Please check Server logs for details.");
      return false;
    } catch (IOException e) {
      LOG.error("Can't dump Sqoop repository to file:", e);
      System.out.println("Writing repository dump to file failed. Please check Server logs for details.");
      return false;
    }
    return true;

  }

  private JSONObject dump(boolean skipSensitive) {

    RepositoryManager.getInstance().initialize(true);
    ConnectorManager.getInstance().initialize();

    Repository repository = RepositoryManager.getInstance().getRepository();


    JSONObject result = new JSONObject();

    LOG.info("Dumping Links with skipSensitive=" + String.valueOf(skipSensitive));
    List<MLink> links = repository.findLinks();
    LinksBean linkBeans = new LinksBean(links);
    JSONObject linksJsonObject = linkBeans.extract(skipSensitive);
    JSONArray linksJsonArray = (JSONArray)linksJsonObject.get(JSONConstants.LINKS);
    addConnectorName(linksJsonArray, JSONConstants.CONNECTOR_ID, JSONConstants.CONNECTOR_NAME);
    result.put(JSONConstants.LINKS, linksJsonObject);

    LOG.info("Dumping Jobs with skipSensitive=" + String.valueOf(skipSensitive));
    JobsBean jobs = new JobsBean(repository.findJobs());
    JSONObject jobsJsonObject = jobs.extract(skipSensitive);
    JSONArray jobsJsonArray = (JSONArray)jobsJsonObject.get(JSONConstants.JOBS);
    addConnectorName(jobsJsonArray, JSONConstants.FROM_CONNECTOR_ID, JSONConstants.FROM_CONNECTOR_NAME);
    addConnectorName(jobsJsonArray, JSONConstants.TO_CONNECTOR_ID, JSONConstants.TO_CONNECTOR_NAME);
    addLinkName(jobsJsonArray, JSONConstants.FROM_LINK_ID, JSONConstants.FROM_LINK_NAME);
    addLinkName(jobsJsonArray, JSONConstants.TO_LINK_ID, JSONConstants.TO_LINK_NAME);
    result.put(JSONConstants.JOBS, jobsJsonObject);

    LOG.info("Dumping Submissions with skipSensitive=" + String.valueOf(skipSensitive));
    SubmissionsBean submissions = new SubmissionsBean(repository.findSubmissions());
    JSONObject submissionsJsonObject = submissions.extract(skipSensitive);
    JSONArray submissionsJsonArray = (JSONArray)submissionsJsonObject.get(JSONConstants.SUBMISSIONS);
    addJobName(submissionsJsonArray, JSONConstants.JOB_ID);
    result.put(JSONConstants.SUBMISSIONS, submissionsJsonObject);

    result.put(JSONConstants.METADATA, repoMetadata(skipSensitive));

    return result;
  }

  private JSONObject repoMetadata(boolean skipSensitive) {
    JSONObject metadata = new JSONObject();
    metadata.put(JSONConstants.VERSION, VersionInfo.getBuildVersion());
    metadata.put(JSONConstants.REVISION, VersionInfo.getSourceRevision());
    metadata.put(JSONConstants.COMPILE_DATE, VersionInfo.getBuildDate());
    metadata.put(JSONConstants.COMPILE_USER, VersionInfo.getUser());
    metadata.put(JSONConstants.INCLUDE_SENSITIVE,!skipSensitive );

    return metadata;
  }

  private JSONArray addConnectorName(JSONArray jsonArray, String connectorKey, String connectorName) {
    ConnectorManager connectorManager = ConnectorManager.getInstance();

    Iterator<JSONObject> iterator = jsonArray.iterator();

    while (iterator.hasNext()) {
      JSONObject result = iterator.next();
      Long connectorId = (Long) result.get(connectorKey);
      result.put(connectorName,  connectorManager.getConnectorConfigurable(connectorId).getUniqueName());
    }

    return jsonArray;
  }

  private JSONArray addLinkName(JSONArray jsonArray, String linkKey, String linkName) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    Iterator<JSONObject> iterator = jsonArray.iterator();

    while (iterator.hasNext()) {
      JSONObject jobObject = iterator.next();
      Long linkId = (Long) jobObject.get(linkKey);
      jobObject.put(linkName, repository.findLink(linkId).getName());
    }

    return jsonArray;
  }

  private JSONArray addJobName(JSONArray jsonArray, String jobKey) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    Iterator<JSONObject> iterator = jsonArray.iterator();

    while (iterator.hasNext()) {
      JSONObject submissionObject = iterator.next();
      Long jobId = (Long) submissionObject.get(jobKey);
      submissionObject.put(JSONConstants.JOB_NAME,  repository.findJob(jobId).getName());
    }

    return jsonArray;
  }
}
