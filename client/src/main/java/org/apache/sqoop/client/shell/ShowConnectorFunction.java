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
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.client.request.ConnectorRequest;
import org.apache.sqoop.json.ConnectorBean;
import org.codehaus.groovy.tools.shell.IO;

@SuppressWarnings("serial")
public class ShowConnectorFunction extends SqoopFunction
{
  public static final String ALL = "all";
  public static final String CID = "cid";

  private IO io;
  private ConnectorRequest conntectorRequest;

  @SuppressWarnings("static-access")
  protected ShowConnectorFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
        .withDescription("Display all connectors")
        .withLongOpt(ALL)
        .create(ALL.charAt(0)));
    this.addOption(OptionBuilder.hasArg().withArgName("cid")
        .withDescription(  "Display the connector with cid" )
        .withLongOpt(CID)
        .create(CID.charAt(0)));
  }

  public void printHelp(PrintWriter out) {
    out.println("Usage: show connector");
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
      showConnector(null);

    } else if (line.hasOption(CID)) {
      showConnector(line.getOptionValue(CID));
    }

    return null;
  }

  private void showConnector(String cid) {
    if (conntectorRequest == null) {
      conntectorRequest = new ConnectorRequest();
    }
    ConnectorBean connectorBean =
      conntectorRequest.doGet(Environment.getServerUrl(), cid);
    long[] ids = connectorBean.getIds();
    String[] names = connectorBean.getNames();
    String[] classes = connectorBean.getClasses();

    if (cid == null) {
      io.out.println("@|bold Metadata for all connectors:|@");  
      int size = ids.length;
      for (int i = 0; i < size; i++) {
        io.out.print("Connector ");
        io.out.print(i+1);
        io.out.println(":");
  
        io.out.print("  Id: ");
        io.out.println(ids[i]);
        io.out.print("  Name: ");
        io.out.println(names[i]);
        io.out.print("  Class: ");
        io.out.println(classes[i]);
      }

    } else {
      io.out.println("@|bold Metadata for the connector:|@");
      io.out.print("  Id: ");
      io.out.println(ids[0]);
      io.out.print("  Name: ");
      io.out.println(names[0]);
      io.out.print("  Class: ");
      io.out.println(classes[0]);
    }

    io.out.println();
  }
}