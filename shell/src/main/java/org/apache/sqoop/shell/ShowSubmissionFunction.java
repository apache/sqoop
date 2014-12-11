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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.SubmissionDisplayer;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import static org.apache.sqoop.shell.ShellEnvironment.*;

@SuppressWarnings("serial")
public class ShowSubmissionFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public ShowSubmissionFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_SUBMISSIONS))
        .withLongOpt(Constants.OPT_DETAIL)
        .create(Constants.OPT_DETAIL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_JID)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_SUBMISSIONS_JOB_ID))
        .withLongOpt(Constants.OPT_JID)
        .create(Constants.OPT_JID_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_DETAIL)) {
      if (line.hasOption(Constants.OPT_JID)) {
        showSubmissions(getLong(line, Constants.OPT_JID));
      } else {
        showSubmissions(null);
      }
    } else {
      if (line.hasOption(Constants.OPT_JID)) {
        showSummary(getLong(line, Constants.OPT_JID));
      } else {
        showSummary(null);
      }
    }

    return Status.OK;
  }

  private void showSummary(Long jid) {
    List<MSubmission> submissions;
    if (jid == null) {
      submissions = client.getSubmissions();
    } else {
      submissions = client.getSubmissionsForJob(jid);
    }

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_JOB_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_EXTERNAL_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_STATUS));
    header.add(resourceString(Constants.RES_TABLE_HEADER_DATE));

    List<String> jids = new LinkedList<String>();
    List<String> eids = new LinkedList<String>();
    List<String> status = new LinkedList<String>();
    List<String> dates = new LinkedList<String>();

    for (MSubmission submission : submissions) {
      jids.add(String.valueOf(submission.getJobId()));
      eids.add(String.valueOf(submission.getExternalJobId()));
      status.add(submission.getStatus().toString());
      dates.add(submission.getLastUpdateDate().toString());
    }

    TableDisplayer.display(header, jids, eids, status, dates);
  }

  private void showSubmissions(Long jid) {
    List<MSubmission> submissions;
    if (jid == null) {
      submissions = client.getSubmissions();
    } else {
      submissions = client.getSubmissionsForJob(jid);
    }

    for (MSubmission submission : submissions) {
      SubmissionDisplayer.displaySubmission(submission);
    }
  }
}
