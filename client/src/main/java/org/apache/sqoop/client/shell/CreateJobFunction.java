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
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.ConnectionBean;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.IO;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.ResourceBundle;

import static org.apache.sqoop.client.utils.FormFiller.*;
import static org.apache.sqoop.client.core.RequestCache.*;

/**
 * Handles creation of new job objects.
 */
public class CreateJobFunction extends  SqoopFunction {


  private IO io;

  @SuppressWarnings("static-access")
  public CreateJobFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
      .withDescription(getResource().getString(Constants.RES_PROMPT_CONN_ID))
      .withLongOpt(Constants.OPT_XID)
      .hasArg()
      .create(Constants.OPT_XID_CHAR)
    );
    this.addOption(OptionBuilder
      .withDescription(getResource().getString(Constants.RES_PROMPT_JOB_TYPE))
      .withLongOpt(Constants.OPT_TYPE)
      .hasArg()
      .create(Constants.OPT_TYPE_CHAR)
    );
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (!line.hasOption(Constants.OPT_XID)) {
      io.out.println(getResource().getString(Constants.RES_ARGS_XID_MISSING));
      return null;
    }
    if (!line.hasOption(Constants.OPT_TYPE)) {
      io.out.println(getResource().getString(Constants.RES_ARGS_TYPE_MISSING));
      return null;
    }

    try {
      createJob(line.getOptionValue(Constants.OPT_XID),
          line.getOptionValue(Constants.OPT_TYPE));
    } catch (IOException ex) {
      throw new SqoopException(ClientError.CLIENT_0005, ex);
    }

    return null;
  }

  private void createJob(String connectionId, String type) throws IOException {
    io.out.println(MessageFormat.format(getResource().getString(Constants
        .RES_CREATE_CREATING_JOB), connectionId));

    ConsoleReader reader = new ConsoleReader();

    FrameworkBean frameworkBean = readFramework();
    ConnectionBean connectionBean = readConnection(connectionId);
    ConnectorBean connectorBean;

    MFramework framework = frameworkBean.getFramework();
    ResourceBundle frameworkBundle = frameworkBean.getResourceBundle();

    MConnection connection = connectionBean.getConnections().get(0);

    connectorBean = readConnector(String.valueOf(connection.getConnectorId()));
    MConnector connector = connectorBean.getConnectors().get(0);
    ResourceBundle connectorBundle = connectorBean.getResourceBundles().get(connector.getPersistenceId());

    MJob.Type jobType = MJob.Type.valueOf(type.toUpperCase());

    MJob job = new MJob(
      connector.getPersistenceId(),
      connection.getPersistenceId(),
      jobType,
      connector.getJobForms(jobType),
      framework.getJobForms(jobType)
    );

    Status status = Status.FINE;

    io.out.println(getResource().getString(Constants.RES_PROMPT_FILL_JOB_METADATA));

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
      status = createJobApplyValidations(job);
    } while(!status.canProceed());

    io.out.println(MessageFormat.format(getResource()
        .getString(Constants.RES_CREATE_JOB_SUCCESSFUL), status.name(),
        job.getPersistenceId()));
  }
}
