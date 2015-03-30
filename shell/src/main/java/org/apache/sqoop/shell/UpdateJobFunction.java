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
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.ConfigDisplayer;
import org.apache.sqoop.shell.utils.ConfigOptions;
import org.apache.sqoop.shell.utils.JobDynamicConfigOptions;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigFiller.*;

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
      .isRequired()
      .hasArg()
      .create(Constants.OPT_JID_CHAR));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return updateJob(getLong(line, Constants.OPT_JID), line.getArgList(), isInteractive);
  }

  private Status updateJob(Long jobId, List<String> args, boolean isInteractive) throws IOException {
    printlnResource(Constants.RES_SQOOP_UPDATING_JOB, jobId);

    ConsoleReader reader = new ConsoleReader();

    // TODO(SQOOP-1634): using from/to and driver config id, this call can be avoided
    MJob job = client.getJob(jobId);

    ResourceBundle fromConnectorBundle = client.getConnectorConfigBundle(
        job.getFromConnectorId());
    ResourceBundle toConnectorBundle = client.getConnectorConfigBundle(
        job.getToConnectorId());
    ResourceBundle driverConfigBundle = client.getDriverConfigBundle();

    Status status = Status.OK;

    if (isInteractive) {
      printlnResource(Constants.RES_PROMPT_UPDATE_JOB_CONFIG);

      do {
        // Print error introduction if needed
        if( !status.canProceed() ) {
          errorIntroduction();
        }

        // Fill in data from user
        if(!fillJobWithBundle(reader, job, fromConnectorBundle, toConnectorBundle, driverConfigBundle)) {
          return status;
        }

        // Try to create
        status = client.updateJob(job);
      } while(!status.canProceed());
    } else {
      JobDynamicConfigOptions options = new JobDynamicConfigOptions();
      options.prepareOptions(job);
      CommandLine line = ConfigOptions.parseOptions(options, 0, args, false);
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

    ConfigDisplayer.displayConfigWarning(job);
    printlnResource(Constants.RES_UPDATE_JOB_SUCCESSFUL, status.name());

    return status;
  }
}
