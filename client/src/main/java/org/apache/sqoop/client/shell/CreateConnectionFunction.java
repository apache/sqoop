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
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
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
public class CreateConnectionFunction extends SqoopFunction {

  private static final String CID = "cid";

  private IO io;

  @SuppressWarnings("static-access")
  public CreateConnectionFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
      .withDescription("Connector ID")
      .withLongOpt(CID)
      .hasArg()
      .create(CID.charAt(0)));
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (!line.hasOption(CID)) {
      io.out.println("Required argument --cid is missing.");
      return null;
    }

    try {
      createConnection(line.getOptionValue(CID));
    } catch (IOException ex) {
      throw new SqoopException(ClientError.CLIENT_0005, ex);
    }

    return null;
  }

  private void createConnection(String connectorId) throws IOException {
    io.out.println("Creating connection for connector with id " + connectorId);

    ConsoleReader reader = new ConsoleReader();

    FrameworkBean frameworkBean = readFramework();
    ConnectorBean connectorBean = readConnector(connectorId);

    MFramework framework = frameworkBean.getFramework();
    ResourceBundle frameworkBundle = frameworkBean.getResourceBundle();

    MConnector connector = connectorBean.getConnectors().get(0);
    ResourceBundle connectorBundle = connectorBean.getResourceBundles().get(0);

    MConnection connection = new MConnection(connector.getPersistenceId(),
                                             connector.getConnectionForms(),
                                             framework.getConnectionForms());

    Status status = Status.FINE;

    io.out.println("Please fill following values to create new connection"
      + " object");

    do {
      // Print error introduction if needed
      if( !status.canProceed() ) {
        errorIntroduction(io);
      }

      // Fill in data from user
      if(!fillConnection(io, reader, connection,
                         connectorBundle, frameworkBundle)) {
        return;
      }

      // Try to create
      status = createConnectionApplyValidations(connection);
    } while(!status.canProceed());

    io.out.println("New connection was successfully created with validation "
      + "status " + status.name() + " and persistent id "
      + connection.getPersistenceId());
  }


}
