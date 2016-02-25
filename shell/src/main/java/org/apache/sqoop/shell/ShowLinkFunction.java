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
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.validation.Status;

import java.text.DateFormat;
import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigDisplayer.*;

/**
 *
 */
@SuppressWarnings("serial")
public class ShowLinkFunction extends SqoopFunction {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("static-access")
  public ShowLinkFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_LINKS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_NAME)
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_LINK_LN))
        .withLongOpt(Constants.OPT_NAME)
        .create(Constants.OPT_NAME_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showLinks();
    } else if (line.hasOption(Constants.OPT_NAME)) {
      showLink(line.getOptionValue(Constants.OPT_NAME));
    } else {
      showSummary();
    }

    return Status.OK;
  }

  private void showSummary() {
    List<MLink> links = client.getLinks();

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_CONNECTOR_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_ENABLED));

    List<String> names = new LinkedList<String>();
    List<String> connectorNames = new LinkedList<String>();
    List<String> availabilities = new LinkedList<String>();

    for (MLink link : links) {
      names.add(link.getName());
      connectorNames.add(link.getConnectorName());
      availabilities.add(String.valueOf(link.getEnabled()));
    }

    displayTable(header, names, connectorNames, availabilities);
  }

  private void showLinks() {
    List<MLink> links = client.getLinks();

    printlnResource(Constants.RES_SHOW_PROMPT_LINKS_TO_SHOW, links.size());

    for (MLink link : links) {
      displayLink(link);
    }
  }

  private void showLink(String linkArg) {
    MLink link = client.getLink(linkArg);

    printlnResource(Constants.RES_SHOW_PROMPT_LINKS_TO_SHOW, 1);

    displayLink(link);
  }

  private void displayLink(MLink link) {
    DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

    printlnResource(Constants.RES_SHOW_PROMPT_LINK_INFO,
      link.getName(),
      link.getEnabled(),
      link.getCreationUser(),
      formatter.format(link.getCreationDate()),
      link.getLastUpdateUser(),
      formatter.format(link.getLastUpdateDate())
    );

    printlnResource(Constants.RES_SHOW_PROMPT_LINK_CN_INFO, link.getConnectorName());

    // Display link config
    displayConfig(link.getConnectorLinkConfig().getConfigs(),
      client.getConnectorConfigBundle(link.getConnectorName()));
  }
}
