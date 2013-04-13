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
package org.apache.sqoop.client.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.client.utils.TableDisplayer;
import org.apache.sqoop.model.MJob;

import java.text.DateFormat;
import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;
import static org.apache.sqoop.client.utils.FormDisplayer.*;

/**
 *
 */
public class ShowJobFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  protected ShowJobFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_JOBS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_JID)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_JOB_JID))
        .withLongOpt(Constants.OPT_JID)
        .create(Constants.OPT_JID_CHAR));
  }

  public Object executeFunction(CommandLine line) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showJobs();
    } else if (line.hasOption(Constants.OPT_JID)) {
      showJob(getLong(line, Constants.OPT_JID));
    } else {
      showSummary();
    }

    return null;
  }

  private void showSummary() {
    List<MJob> jobs = client.getJobs();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_TYPE));
    header.add(resourceString(Constants.RES_TABLE_HEADER_CONNECTOR));

    List<String> ids = new LinkedList<String>();
    List<String> names = new LinkedList<String>();
    List<String> types = new LinkedList<String>();
    List<String> connectors = new LinkedList<String>();

    for(MJob job : jobs) {
      ids.add(String.valueOf(job.getPersistenceId()));
      names.add(job.getName());
      types.add(job.getType().toString());
      connectors.add(String.valueOf(job.getConnectorId()));
    }

    TableDisplayer.display(header, ids, names, types, connectors);
  }

  private void showJobs() {
    List<MJob> jobs = client.getJobs();
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
      formatter.format(job.getCreationDate()),
      formatter.format(job.getLastUpdateDate())
    );
    printlnResource(Constants.RES_SHOW_PROMPT_JOB_XID_CID_INFO,
        job.getConnectionId(),
        job.getConnectorId());

    // Display connector part
    displayForms(job.getConnectorPart().getForms(),
                 client.getResourceBundle(job.getConnectorId()));
    displayForms(job.getFrameworkPart().getForms(),
                 client.getFrameworkResourceBundle());
  }
}
