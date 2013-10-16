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

import jline.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.FormDisplayer;
import org.apache.sqoop.shell.utils.FormOptions;
import org.apache.sqoop.shell.utils.JobDynamicFormOptions;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.FormFiller.*;

/**
 *
 */
@SuppressWarnings("serial")
public class UpdateJobFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public UpdateJobFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_JOB_ID))
      .withLongOpt(Constants.OPT_JID)
      .hasArg()
      .create(Constants.OPT_JID_CHAR));
  }

  @Override
  public boolean validateArgs(CommandLine line) {
    if (!line.hasOption(Constants.OPT_JID)) {
      printlnResource(Constants.RES_ARGS_JID_MISSING);
      return false;
    }
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return updateJob(getLong(line, Constants.OPT_JID), line.getArgList(), isInteractive);
  }

  private Status updateJob(Long jobId, List<String> args, boolean isInteractive) throws IOException {
    printlnResource(Constants.RES_UPDATE_UPDATING_JOB, jobId);

    ConsoleReader reader = new ConsoleReader();

    MJob job = client.getJob(jobId);

    ResourceBundle connectorBundle = client.getResourceBundle(job.getConnectorId());
    ResourceBundle frameworkBundle = client.getFrameworkResourceBundle();

    Status status = Status.FINE;

    if (isInteractive) {
      printlnResource(Constants.RES_PROMPT_UPDATE_JOB_METADATA);

      do {
        // Print error introduction if needed
        if( !status.canProceed() ) {
          errorIntroduction();
        }

        // Fill in data from user
        if(!fillJob(reader, job, connectorBundle, frameworkBundle)) {
          return status;
        }

        // Try to create
        status = client.updateJob(job);
      } while(!status.canProceed());
    } else {
      JobDynamicFormOptions options = new JobDynamicFormOptions();
      options.prepareOptions(job);
      CommandLine line = FormOptions.parseOptions(options, 0, args, false);
      if (fillJob(line, job)) {
        status = client.updateJob(job);
        if (!status.canProceed()) {
          printJobValidationMessages(job);
          return status;
        }
      } else {
        printJobValidationMessages(job);
        return null;
      }
    }

    FormDisplayer.displayFormWarning(job);
    printlnResource(Constants.RES_UPDATE_JOB_SUCCESSFUL, status.name());

    return status;
  }
}
