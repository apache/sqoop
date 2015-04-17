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
package org.apache.sqoop.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import java.text.DateFormat;
import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigDisplayer.*;

/**
 *
 */
@SuppressWarnings("serial")
public class ShowJobFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public ShowJobFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_JOBS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_CID)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_JOBS_CID))
        .withLongOpt(Constants.OPT_CID)
        .create(Constants.OPT_CID_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_JID)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_JOB_JID))
        .withLongOpt(Constants.OPT_JID)
        .create(Constants.OPT_JID_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showJobs(null);
    } else if (line.hasOption(Constants.OPT_CID)) {
      showJobs(getLong(line, Constants.OPT_CID));
    } else if (line.hasOption(Constants.OPT_JID)) {
      showJob(getLong(line, Constants.OPT_JID));
    } else {
      showSummary();
    }
    return Status.OK;
  }

  private void showSummary() {
    List<MJob> jobs = client.getJobs();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_FROM_CONNECTOR));
    header.add(resourceString(Constants.RES_TABLE_HEADER_TO_CONNECTOR));
    header.add(resourceString(Constants.RES_TABLE_HEADER_ENABLED));

    List<String> ids = new LinkedList<String>();
    List<String> names = new LinkedList<String>();
    List<String> fromConnectors = new LinkedList<String>();
    List<String> toConnectors = new LinkedList<String>();
    List<String> availabilities = new LinkedList<String>();

    for(MJob job : jobs) {
      ids.add(String.valueOf(job.getPersistenceId()));
      names.add(job.getName());
      fromConnectors.add(String.valueOf(
          job.getFromConnectorId()));
      toConnectors.add(String.valueOf(
          job.getToConnectorId()));
      availabilities.add(String.valueOf(job.getEnabled()));
    }

    TableDisplayer.display(header, ids, names, fromConnectors, toConnectors, availabilities);
  }

  private void showJobs(Long id) {
    List<MJob> jobs;
    if (id == null) {
      jobs = client.getJobs();
    } else {
      jobs = client.getJobsByConnector(id);
    }
    printlnResource(Constants.RES_SHOW_PROMPT_JOBS_TO_SHOW, jobs.size());

    for (MJob job : jobs) {
      displayJob(job);
    }
  }

  private void showJob(Long jid) {
    MJob job = client.getJob(jid);
    printlnResource(Constants.RES_SHOW_PROMPT_JOBS_TO_SHOW, 1);

    displayJob(job);
  }

  private void displayJob(MJob job) {
    DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

    printlnResource(
      Constants.RES_SHOW_PROMPT_JOB_INFO,
      job.getPersistenceId(),
      job.getName(),
      job.getEnabled(),
      job.getCreationUser(),
      formatter.format(job.getCreationDate()),
      job.getLastUpdateUser(),
      formatter.format(job.getLastUpdateDate())
    );
    displayConfig(job.getDriverConfig().getConfigs(),
            client.getDriverConfigBundle());
    printlnResource(Constants.RES_SHOW_PROMPT_JOB_FROM_LID_INFO,
        job.getFromLinkId());
    displayConfig(job.getFromJobConfig().getConfigs(),
                 client.getConnectorConfigBundle(job.getFromConnectorId()));
    printlnResource(Constants.RES_SHOW_PROMPT_JOB_TO_LID_INFO,
            job.getToLinkId());
    displayConfig(job.getToJobConfig().getConfigs(),
                 client.getConnectorConfigBundle(job.getToConnectorId()));
  }
}
