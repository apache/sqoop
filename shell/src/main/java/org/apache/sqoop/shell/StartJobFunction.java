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

import static org.apache.sqoop.shell.ShellEnvironment.client;
import static org.apache.sqoop.shell.ShellEnvironment.getPollTimeout;
import static org.apache.sqoop.shell.ShellEnvironment.resourceString;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.SubmissionDisplayer;
import org.apache.sqoop.validation.Status;

@SuppressWarnings("serial")
public class StartJobFunction extends SqoopFunction {
  public static final Logger LOG = Logger.getLogger(StartJobFunction.class);

  @SuppressWarnings("static-access")
  public StartJobFunction() {
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_JID)
       .withDescription(resourceString(Constants.RES_PROMPT_JOB_ID))
       .withLongOpt(Constants.OPT_JID)
       .create(Constants.OPT_JID_CHAR));
    this.addOption(OptionBuilder
       .withDescription(resourceString(Constants.RES_PROMPT_SYNCHRONOUS))
       .withLongOpt(Constants.OPT_SYNCHRONOUS)
       .create(Constants.OPT_SYNCHRONOUS_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    // Poll until finished
    if (line.hasOption(Constants.OPT_SYNCHRONOUS) && line.hasOption(Constants.OPT_JID)) {
      long pollTimeout = getPollTimeout();
      SubmissionCallback callback = new SubmissionCallback() {
        @Override
        public void submitted(MSubmission submission) {
          SubmissionDisplayer.displayHeader(submission);
          SubmissionDisplayer.displayProgress(submission);
        }

        @Override
        public void updated(MSubmission submission) {
          SubmissionDisplayer.displayProgress(submission);
        }

        @Override
        public void finished(MSubmission submission) {
          SubmissionDisplayer.displayFooter(submission);
        }
      };

      try {
        client.startJob(getLong(line, Constants.OPT_JID), callback, pollTimeout);
      } catch (InterruptedException e) {
        throw new SqoopException(ShellError.SHELL_0007, e);
      }
    } else if (line.hasOption(Constants.OPT_JID)) {
      MSubmission submission = client.startJob(getLong(line, Constants.OPT_JID));
      if(submission.getStatus().isFailure()) {
        SubmissionDisplayer.displayFooter(submission);
      } else {
        SubmissionDisplayer.displayHeader(submission);
        SubmissionDisplayer.displayProgress(submission);
      }
    } else {
      return null;
    }

    return Status.OK;
  }
}
