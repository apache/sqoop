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

import jline.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.core.ClientError;
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.client.utils.FormDisplayer;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.ResourceBundle;

import static org.apache.sqoop.client.utils.FormFiller.*;
import static org.apache.sqoop.client.shell.ShellEnvironment.*;

/**
 *
 */
public class UpdateJobFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public UpdateJobFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_JOB_ID))
      .withLongOpt(Constants.OPT_JID)
      .hasArg()
      .create(Constants.OPT_JID_CHAR));
  }

  public Object executeFunction(CommandLine line) {
    if (!line.hasOption(Constants.OPT_JID)) {
      printlnResource(Constants.RES_ARGS_JID_MISSING);
      return null;
    }

    try {
      updateJob(getLong(line, Constants.OPT_JID));
    } catch (IOException ex) {
      throw new SqoopException(ClientError.CLIENT_0005, ex);
    }

    return null;
  }

  private void updateJob(Long jobId) throws IOException {
    printlnResource(Constants.RES_UPDATE_UPDATING_JOB, jobId);

    ConsoleReader reader = new ConsoleReader();

    MJob job = client.getJob(jobId);

    ResourceBundle connectorBundle = client.getResourceBundle(job.getConnectorId());
    ResourceBundle frameworkBundle = client.getFrameworkResourceBundle();

    Status status = Status.FINE;

    printlnResource(Constants.RES_PROMPT_UPDATE_JOB_METADATA);

    do {
      // Print error introduction if needed
      if( !status.canProceed() ) {
        errorIntroduction();
      }

      // Fill in data from user
      if(!fillJob(reader, job, connectorBundle, frameworkBundle)) {
        return;
      }

      // Try to create
      status = client.updateJob(job);
    } while(!status.canProceed());
    FormDisplayer.displayFormWarning(job);
    printlnResource(Constants.RES_UPDATE_JOB_SUCCESSFUL, status.name());
  }
}
