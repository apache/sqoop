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
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.IO;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import static org.apache.sqoop.client.utils.FormFiller.*;
import static org.apache.sqoop.client.core.RequestCache.*;

/**
 *
 */
public class UpdateJobFunction extends SqoopFunction {

  private static final String JID = "jid";

  private IO io;

  @SuppressWarnings("static-access")
  public UpdateJobFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
      .withDescription("Job ID")
      .withLongOpt(JID)
      .hasArg()
      .create(JID.charAt(0)));
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (!line.hasOption(JID)) {
      io.out.println("Required argument --jid is missing.");
      return null;
    }

    try {
      updateJob(line.getOptionValue(JID));
    } catch (IOException ex) {
      throw new SqoopException(ClientError.CLIENT_0005, ex);
    }

    return null;
  }

  private void updateJob(String jobId) throws IOException {
    io.out.println("Updating job with id " + jobId);

    ConsoleReader reader = new ConsoleReader();

    JobBean jobBean = readJob(jobId);

    // TODO(jarcec): Check that we have expected data
    MJob job = jobBean.getJobs().get(0);
    ResourceBundle frameworkBundle
      = jobBean.getFrameworkBundle();
    ResourceBundle connectorBundle
      = jobBean.getConnectorBundle(job.getConnectorId());

    Status status = Status.FINE;

    io.out.println("Please update job metadata:");

    do {
      // Print error introduction if needed
      if( !status.canProceed() ) {
        errorIntroduction(io);
      }

      // Fill in data from user
      if(!fillJob(io, reader, job, connectorBundle, frameworkBundle)) {
        return;
      }

      // Try to create
      status = updateJobApplyValidations(job);
    } while(!status.canProceed());

    io.out.println("Job was successfully updated with status "
      + status.name());
  }
}
