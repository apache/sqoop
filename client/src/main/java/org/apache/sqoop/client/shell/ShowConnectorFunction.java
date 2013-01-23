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

import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.client.utils.TableDisplayer;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.model.MConnector;
import org.codehaus.groovy.tools.shell.IO;

import static org.apache.sqoop.client.utils.FormDisplayer.*;
import static org.apache.sqoop.client.core.RequestCache.*;

@SuppressWarnings("serial")
public class ShowConnectorFunction extends SqoopFunction
{

  private IO io;


  @SuppressWarnings("static-access")
  protected ShowConnectorFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_ALL_CONNECTORS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName("cid")
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_CONNECTOR_CID))
        .withLongOpt(Constants.OPT_CID)
        .create(Constants.OPT_CID_CHAR));
  }

  public void printHelp(PrintWriter out) {
    out.println(getResource().getString(Constants.RES_SHOW_CONNECTOR_USAGE));
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (line.hasOption(Constants.OPT_ALL)) {
      showConnector(null);
    } else if (line.hasOption(Constants.OPT_CID)) {
      showConnector(line.getOptionValue(Constants.OPT_CID));
    } else {
      showSummary();
    }

    return null;
  }

  private void showSummary() {
    ConnectorBean connectorBean = readConnector(null);
    List<MConnector> connectors = connectorBean.getConnectors();

    List<String> header = new LinkedList<String>();
    header.add(getResource().getString(Constants.RES_TABLE_HEADER_ID));
    header.add(getResource().getString(Constants.RES_TABLE_HEADER_NAME));
    header.add(getResource().getString(Constants.RES_TABLE_HEADER_VERSION));
    header.add(getResource().getString(Constants.RES_TABLE_HEADER_CLASS));

    List<String> ids = new LinkedList<String>();
    List<String> uniqueNames = new LinkedList<String>();
    List<String> versions = new LinkedList<String>();
    List<String> classes = new LinkedList<String>();

    for(MConnector connector : connectors) {
      ids.add(String.valueOf(connector.getPersistenceId()));
      uniqueNames.add(connector.getUniqueName());
      versions.add(connector.getVersion());
      classes.add(connector.getClassName());
    }

    TableDisplayer.display(io, header, ids, uniqueNames, versions, classes);
  }

  private void showConnector(String cid) {
    ConnectorBean connectorBean = readConnector(cid);
    List<MConnector> connectors = connectorBean.getConnectors();
    Map<Long, ResourceBundle> bundles = connectorBean.getResourceBundles();
    String s = MessageFormat.format(getResource().getString(Constants
       .RES_SHOW_PROMPT_CONNECTORS_TO_SHOW), connectors.size());
    io.out.println(s);
    for (MConnector connector : connectors) {
      s =  MessageFormat.format(getResource().getString(Constants
        .RES_SHOW_PROMPT_CONNECTOR_INFO), connector.getPersistenceId(),
        connector.getUniqueName(), connector.getClassName(),
        connector.getVersion());
      io.out.println(StringEscapeUtils.unescapeJava(s));
      displayFormMetadataDetails(io, connector, bundles.get(connector.getPersistenceId()));
    }

    io.out.println();
  }
}
