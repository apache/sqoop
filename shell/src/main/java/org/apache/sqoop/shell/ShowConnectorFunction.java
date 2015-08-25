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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigDisplayer.*;

@SuppressWarnings("serial")
public class ShowConnectorFunction extends SqoopFunction {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("static-access")
  public ShowConnectorFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_CONNECTORS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName("cid")
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_CONNECTOR_CID))
        .withLongOpt(Constants.OPT_CID)
        .create(Constants.OPT_CID_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showConnectors();
    } else if (line.hasOption(Constants.OPT_CID)) {
      showConnector(line);
    } else {
      showSummary();
    }

    return Status.OK;
  }

  private void showSummary() {
    Collection<MConnector> connectors = client.getConnectors();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_VERSION));
    header.add(resourceString(Constants.RES_TABLE_HEADER_CLASS));
    header.add(resourceString(Constants.RES_TABLE_HEADER_SUPPORTED_DIRECTIONS));

    List<String> ids = new LinkedList<String>();
    List<String> uniqueNames = new LinkedList<String>();
    List<String> versions = new LinkedList<String>();
    List<String> classes = new LinkedList<String>();
    List<String> supportedDirections = new LinkedList<String>();

    for(MConnector connector : connectors) {
      ids.add(String.valueOf(connector.getPersistenceId()));
      uniqueNames.add(connector.getUniqueName());
      versions.add(connector.getVersion());
      classes.add(connector.getClassName());
      supportedDirections.add(connector.getSupportedDirections().toString());
    }

    TableDisplayer.display(header, ids, uniqueNames, versions, classes, supportedDirections);
  }

  private void showConnectors() {
    Collection<MConnector> connectors = client.getConnectors();

    printlnResource(Constants.RES_SHOW_PROMPT_CONNECTORS_TO_SHOW, connectors.size());

    for (MConnector connector : connectors) {
      displayConnector(connector);
    }
  }

  private void showConnector(CommandLine line) {
    //Check if the command argument is a connector name
    String connectorName = line.getOptionValue(Constants.OPT_CID);
    MConnector connector = client.getConnector(connectorName);
    if (null == connector) {
      //Now check if command line argument is a connector id
      //This works as getConnector(String...) does not throw an exception
      Long cid  = getLong(line, Constants.OPT_CID);
      connector = client.getConnector(cid);
    }

    //No null checks here - as before. This is because getConnector(long...)
    //throws an exception if connector is not found.

    printlnResource(Constants.RES_SHOW_PROMPT_CONNECTORS_TO_SHOW, 1);

    displayConnector(connector);
  }

  private void displayConnector(MConnector connector) {
    printlnResource(Constants.RES_SHOW_PROMPT_CONNECTOR_INFO,
      connector.getPersistenceId(),
      connector.getUniqueName(),
      connector.getClassName(),
      connector.getVersion(),
      connector.getSupportedDirections().toString()
    );
    displayConnectorConfigDetails(connector, client.getConnectorConfigBundle(connector.getPersistenceId()));
  }
}
