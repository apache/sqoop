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
import org.apache.sqoop.client.ClientError;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
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
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_NAME)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_CONNECTOR_NAME))
        .withLongOpt(Constants.OPT_NAME)
        .create(Constants.OPT_NAME_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_DIRECTION)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_CONNECTOR_DIRECTION))
        .withLongOpt(Constants.OPT_DIRECTION)
        .create(Constants.OPT_DIRECTION_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showConnectors();
    } else if (line.hasOption(Constants.OPT_NAME)) {
      showConnector(line);
    } else if (line.hasOption(Constants.OPT_DIRECTION)) {
      showConnectorsByDirection(line);
    } else {
      showSummary();
    }

    return Status.OK;
  }

  private void showSummary() {
    Collection<MConnector> connectors = client.getConnectors();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_VERSION));
    header.add(resourceString(Constants.RES_TABLE_HEADER_CLASS));
    header.add(resourceString(Constants.RES_TABLE_HEADER_SUPPORTED_DIRECTIONS));

    List<String> uniqueNames = new LinkedList<String>();
    List<String> versions = new LinkedList<String>();
    List<String> classes = new LinkedList<String>();
    List<String> supportedDirections = new LinkedList<String>();

    for(MConnector connector : connectors) {
      uniqueNames.add(connector.getUniqueName());
      versions.add(connector.getVersion());
      classes.add(connector.getClassName());
      supportedDirections.add(connector.getSupportedDirections().toString());
    }

    displayTable(header, uniqueNames, versions, classes, supportedDirections);
  }

  private void showConnectors() {
    Collection<MConnector> connectors = client.getConnectors();

    printlnResource(Constants.RES_SHOW_PROMPT_CONNECTORS_TO_SHOW, connectors.size());

    for (MConnector connector : connectors) {
      displayConnector(connector);
    }
  }

  private void showConnectorsByDirection(CommandLine line) {
    // Check if the command argument is a connector direction
    String directionString = line.getOptionValue(Constants.OPT_DIRECTION);
    Direction direction;
    switch (directionString) {
      case Constants.OPT_FROM:
        direction = Direction.FROM;
        break;
      case Constants.OPT_TO:
        direction = Direction.TO;
        break;
      default:
        throw new SqoopException(ShellError.SHELL_0003, directionString);
    }

    Collection<MConnector> connectors = client.getConnectorsByDirection(direction);

    printlnResource(Constants.RES_SHOW_PROMPT_CONNECTORS_TO_SHOW, connectors.size());

    for (MConnector connector : connectors) {
      displayConnector(connector);
    }
  }

  private void showConnector(CommandLine line) {
    //Check if the command argument is a connector name
    String connectorName = line.getOptionValue(Constants.OPT_NAME);
    MConnector connector = client.getConnector(connectorName);

    // check if the connector exist
    if (connector == null) {
      throw new SqoopException(ClientError.CLIENT_0003, connectorName);
    }

    printlnResource(Constants.RES_SHOW_PROMPT_CONNECTORS_TO_SHOW, 1);

    displayConnector(connector);
  }

  private void displayConnector(MConnector connector) {
    printlnResource(Constants.RES_SHOW_PROMPT_CONNECTOR_INFO,
      connector.getUniqueName(),
      connector.getClassName(),
      connector.getVersion(),
      connector.getSupportedDirections().toString()
    );
    displayConnectorConfigDetails(connector, client.getConnectorConfigBundle(connector.getUniqueName()));
  }
}
