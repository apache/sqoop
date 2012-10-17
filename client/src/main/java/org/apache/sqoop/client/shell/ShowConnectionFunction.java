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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.json.ConnectionBean;
import org.apache.sqoop.model.MConnection;
import org.codehaus.groovy.tools.shell.IO;

import java.io.PrintWriter;
import java.util.List;

import static org.apache.sqoop.client.utils.FormDisplayer.*;
import static org.apache.sqoop.client.core.RequestCache.*;

/**
 *
 */
public class ShowConnectionFunction extends SqoopFunction {
  public static final String ALL = "all";
  public static final String XID = "xid";

  private IO io;

  @SuppressWarnings("static-access")
  protected ShowConnectionFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
        .withDescription("Display all connections")
        .withLongOpt(ALL)
        .create(ALL.charAt(0)));
    this.addOption(OptionBuilder.hasArg().withArgName("xid")
        .withDescription(  "Display the connection with xid" )
        .withLongOpt(XID)
        .create('x'));
  }

  public void printHelp(PrintWriter out) {
    out.println("Usage: show connection");
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    if (args.size() == 1) {
      printHelp(io.out);
      io.out.println();
      return null;
    }

    CommandLine line = parseOptions(this, 1, args);
    if (line.hasOption(ALL)) {
      showConnection(null);

    } else if (line.hasOption(XID)) {
      showConnection(line.getOptionValue(XID));
    }

    return null;
  }

  private void showConnection(String xid) {
    ConnectionBean connectionBean = readConnection(xid);

    List<MConnection> connections = connectionBean.getConnections();

    io.out.println("@|bold " + connections.size()
      + " connection(s) to show: |@");

    for (MConnection connection : connections) {
      io.out.println("Connection with id " + connection.getPersistenceId()
        + " and name: " + connection.getName());

      long connectorId = connection.getConnectorId();

      // Display connector part
      displayForms(io,
                   connection.getConnectorPart().getForms(),
                   connectionBean.getConnectorBundle(connectorId));
      displayForms(io,
                   connection.getFrameworkPart().getForms(),
                   connectionBean.getFrameworkBundle());
    }
  }
}
