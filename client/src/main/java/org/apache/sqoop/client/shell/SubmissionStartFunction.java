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
import org.apache.log4j.Logger;
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.client.utils.SubmissionDisplayer;
import org.apache.sqoop.model.MSubmission;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

/**
 *
 */
public class SubmissionStartFunction extends SqoopFunction {
  public static final Logger LOG = Logger.getLogger(SubmissionStartFunction.class);
  public static final long POLL_TIMEOUT = 10000;

  @SuppressWarnings("static-access")
  public SubmissionStartFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_JOB_ID))
      .withLongOpt(Constants.OPT_JID)
      .hasArg()
      .create(Constants.OPT_JID_CHAR));
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_SYNCHRONOUS))
      .withLongOpt(Constants.OPT_SYNCHRONOUS)
      .create());
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_POLL_TIMEOUT))
      .withLongOpt(Constants.OPT_POLL_TIMEOUT)
      .hasArg()
      .create());
  }

  public Object executeFunction(CommandLine line) {
    if (!line.hasOption(Constants.OPT_JID)) {
      printlnResource(Constants.RES_ARGS_JID_MISSING);
      return null;
    }

    MSubmission submission = client.startSubmission(getLong(line, Constants.OPT_JID));
    SubmissionDisplayer.display(submission);

    // Poll until finished
    if (line.hasOption(Constants.OPT_SYNCHRONOUS)) {
      long pollTimeout = POLL_TIMEOUT;
      if (line.hasOption(Constants.OPT_POLL_TIMEOUT)) {
        pollTimeout = Long.getLong(line.getOptionValue(Constants.OPT_POLL_TIMEOUT)).longValue();
      }
      while (submission.getStatus().isRunning()) {
        submission = client.getSubmissionStatus(getLong(line, Constants.OPT_JID));
        SubmissionDisplayer.display(submission);

        // Wait some time
        try {
          Thread.sleep(pollTimeout);
        } catch (InterruptedException e) {
          LOG.error("Could not sleep");
        }
      }
    }
    return null;
  }
}
