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
import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.client.request.ConnectionRequest;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.ConnectionBean;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.IO;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import static org.apache.sqoop.client.utils.FormFiller.*;

/**
 *
 */
public class UpdateConnectionFunction extends SqoopFunction {

  private static final String XID = "xid";

  private ConnectionRequest connectionRequest;

  private IO io;

  @SuppressWarnings("static-access")
  public UpdateConnectionFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
      .withDescription("Connection ID")
      .withLongOpt(XID)
      .hasArg()
      .create(XID.charAt(0)));
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (!line.hasOption(XID)) {
      io.out.println("Required argument --xid is missing.");
      return null;
    }

    try {
      updateConnection(line.getOptionValue(XID));
    } catch (IOException ex) {
      throw new SqoopException(ClientError.CLIENT_0005, ex);
    }

    return null;
  }

  private void updateConnection(String connectionId) throws IOException {
    io.out.println("Updating connection with id " + connectionId);

    ConsoleReader reader = new ConsoleReader();

    ConnectionBean connectionBean = readConnection(connectionId);

    // TODO(jarcec): Check that we have expected data
    MConnection connection = connectionBean.getConnections().get(0);
    ResourceBundle frameworkBundle
      = connectionBean.getFrameworkBundle();
    ResourceBundle connectorBundle
      = connectionBean.getConnectorBundle(connection.getConnectorId());

    Status status = Status.FINE;

    io.out.println("Please update connection metadata:");

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
      status = updateConnection(connection);
    } while(!status.canProceed());

    io.out.println("Connection was successfully updated with status "
      + status.name());
  }

  private Status updateConnection(MConnection connection) {
    if (connectionRequest == null) {
      connectionRequest = new ConnectionRequest();
    }

    return connectionRequest.update(Environment.getServerUrl(), connection);
  }

  private ConnectionBean readConnection(String connectionId) {
    if (connectionRequest == null) {
      connectionRequest = new ConnectionRequest();
    }

    return connectionRequest.read(Environment.getServerUrl(), connectionId);
  }
}
