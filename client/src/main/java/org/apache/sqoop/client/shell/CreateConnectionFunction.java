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
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.ResourceBundle;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;
import static org.apache.sqoop.client.utils.FormFiller.*;

/**
 *
 */
public class CreateConnectionFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public CreateConnectionFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_CONNECTOR_ID))
      .withLongOpt(Constants.OPT_CID)
      .hasArg()
      .create(Constants.OPT_CID_CHAR));
  }

  public Object executeFunction(CommandLine line) {
    if (!line.hasOption(Constants.OPT_CID)) {
      printlnResource(Constants.RES_ARGS_CID_MISSING);
      return null;
    }

    try {
      createConnection(getLong(line, Constants.OPT_CID));
    } catch (IOException ex) {
      throw new SqoopException(ClientError.CLIENT_0005, ex);
    }

    return null;
  }

  private void createConnection(long connectorId) throws IOException {
    printlnResource(Constants.RES_CREATE_CREATING_CONN, connectorId);

    ConsoleReader reader = new ConsoleReader();

    MConnection connection = client.newConnection(connectorId);

    ResourceBundle connectorBundle = client.getResourceBundle(connectorId);
    ResourceBundle frameworkBundle = client.getFrameworkResourceBundle();

    Status status = Status.FINE;
    printlnResource(Constants.RES_PROMPT_FILL_CONN_METADATA);
    do {
      // Print error introduction if needed
      if( !status.canProceed() ) {
        errorIntroduction();
      }

      // Fill in data from user
      if(!fillConnection(reader, connection, connectorBundle, frameworkBundle)) {
        return;
      }

      // Try to create
      status = client.createConnection(connection);
    } while(!status.canProceed());
    FormDisplayer.displayFormWarning(connection);
    printlnResource(Constants.RES_CREATE_CONN_SUCCESSFUL, status.name(), connection.getPersistenceId());
  }
}
