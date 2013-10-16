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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import java.text.DateFormat;
import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.FormDisplayer.*;

/**
 *
 */
@SuppressWarnings("serial")
public class ShowConnectionFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  protected ShowConnectionFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_CONNS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_XID)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_CONN_XID))
        .withLongOpt(Constants.OPT_XID)
        .create(Constants.OPT_XID_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showConnections();
    } else if (line.hasOption(Constants.OPT_XID)) {
      showConnection(getLong(line, Constants.OPT_XID));
    } else {
      showSummary();
    }

    return Status.FINE;
  }

  private void showSummary() {
    List<MConnection> connections = client.getConnections();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_CONNECTOR));
    header.add(resourceString(Constants.RES_TABLE_HEADER_ENABLED));

    List<String> ids = new LinkedList<String>();
    List<String> names = new LinkedList<String>();
    List<String> connectors = new LinkedList<String>();
    List<String> availabilities = new LinkedList<String>();

    for(MConnection connection : connections) {
      ids.add(String.valueOf(connection.getPersistenceId()));
      names.add(connection.getName());
      connectors.add(String.valueOf(connection.getConnectorId()));
      availabilities.add(String.valueOf(connection.getEnabled()));
    }

    TableDisplayer.display(header, ids, names, connectors, availabilities);
  }

  private void showConnections() {
    List<MConnection> connections = client.getConnections();

    printlnResource(Constants.RES_SHOW_PROMPT_CONNS_TO_SHOW, connections.size());

    for (MConnection connection : connections) {
      displayConnection(connection);
    }
  }

  private void showConnection(Long xid) {
    MConnection connection = client.getConnection(xid);

    printlnResource(Constants.RES_SHOW_PROMPT_CONNS_TO_SHOW, 1);

    displayConnection(connection);
  }

  private void displayConnection(MConnection connection) {
    DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

    printlnResource(Constants.RES_SHOW_PROMPT_CONN_INFO,
      connection.getPersistenceId(),
      connection.getName(),
      connection.getEnabled(),
      connection.getCreationUser(),
      formatter.format(connection.getCreationDate()),
      connection.getLastUpdateUser(),
      formatter.format(connection.getLastUpdateDate())
    );

    long connectorId = connection.getConnectorId();
    printlnResource(Constants.RES_SHOW_PROMPT_CONN_CID_INFO, connectorId);

    // Display connector part
    displayForms(connection.getConnectorPart().getForms(),
                 client.getResourceBundle(connectorId));
    displayForms(connection.getFrameworkPart().getForms(),
                 client.getFrameworkResourceBundle());
  }
}
