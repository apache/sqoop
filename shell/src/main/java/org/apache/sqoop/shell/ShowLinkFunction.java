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
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigDisplayer.*;

/**
 *
 */
@SuppressWarnings("serial")
public class ShowLinkFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public ShowLinkFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_LINKS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_FROM)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_LINK_LID))
        .withLongOpt(Constants.OPT_FROM)
        .create(Constants.OPT_LID_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showLinks();
    } else if (line.hasOption(Constants.OPT_LID)) {
      showLink(getLong(line, Constants.OPT_LID));
    } else {
      showSummary();
    }

    return Status.OK;
  }

  private void showSummary() {
    List<MLink> links = client.getLinks();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_CONNECTOR_ID));
    header.add(resourceString(Constants.RES_TABLE_HEADER_CONNECTOR_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_ENABLED));

    List<String> ids = new LinkedList<String>();
    List<String> names = new LinkedList<String>();
    List<String> connectorIds = new LinkedList<String>();
    List<String> availabilities = new LinkedList<String>();

    for (MLink link : links) {
      ids.add(String.valueOf(link.getPersistenceId()));
      names.add(link.getName());
      connectorIds.add(String.valueOf(link.getConnectorId()));
      availabilities.add(String.valueOf(link.getEnabled()));
    }

    List<String> connectorNames = getConnectorNames(connectorIds);

    TableDisplayer.display(header, ids, names, connectorIds, connectorNames, availabilities);
  }

  private void showLinks() {
    List<MLink> links = client.getLinks();

    printlnResource(Constants.RES_SHOW_PROMPT_LINKS_TO_SHOW, links.size());

    for (MLink link : links) {
      displayLink(link);
    }
  }

  private void showLink(Long xid) {
    MLink link = client.getLink(xid);

    printlnResource(Constants.RES_SHOW_PROMPT_LINKS_TO_SHOW, 1);

    displayLink(link);
  }

  private void displayLink(MLink link) {
    DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

    printlnResource(Constants.RES_SHOW_PROMPT_LINK_INFO,
      link.getPersistenceId(),
      link.getName(),
      link.getEnabled(),
      link.getCreationUser(),
      formatter.format(link.getCreationDate()),
      link.getLastUpdateUser(),
      formatter.format(link.getLastUpdateDate())
    );

    long connectorId = link.getConnectorId();
    MConnector connector = client.getConnector(connectorId);
    String connectorName = "";
    if (connector != null) {
      connectorName = connector.getUniqueName();
    }
    printlnResource(Constants.RES_SHOW_PROMPT_LINK_CID_INFO, connectorName, connectorId);

    // Display link config
    displayConfig(link.getConnectorLinkConfig().getConfigs(),
    client.getConnectorConfigBundle(connectorId));
  }

  private List<String> getConnectorNames(List<String> connectorIds) {
    Map<String, String> connectorIdToName = new HashMap<String, String>();
    for (String connectorId : connectorIds) {
      if (!connectorIdToName.containsKey(connectorId)) {
        try {
          MConnector connector = client.getConnector(Long.valueOf(connectorId));
          if (connector != null) {
            connectorIdToName.put(connectorId, connector.getUniqueName());
          }
        } catch (SqoopException ex) {
          connectorIdToName.put(connectorId, "Access Denied");
        }
      }
    }
    List<String> connectorNames = new ArrayList<String>();
    for (String connectorId : connectorIds) {
      if (connectorIdToName.get(connectorId) != null) {
        connectorNames.add(connectorIdToName.get(connectorId));
      } else {
        connectorNames.add("");
      }
    }
    return connectorNames;
  }
}
