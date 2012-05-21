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
import org.codehaus.groovy.tools.shell.IO;

@SuppressWarnings("serial")
public class SetServerFunction extends SqoopFunction
{
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String WEBAPP = "webapp";

  private IO io;

  @SuppressWarnings("static-access")
  protected SetServerFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder.hasArg().withArgName("host")
        .withDescription(  "Host name to invoke server resources" )
        .withLongOpt(HOST)
        .create(HOST.charAt(0)));
    this.addOption(OptionBuilder.hasArg().withArgName("port")
        .withDescription(  "Port number to invoke server resources" )
        .withLongOpt(PORT)
        .create(PORT.charAt(0)));
    this.addOption(OptionBuilder.hasArg().withArgName("webbapp")
        .withDescription(  "Web app to invoke server resources" )
        .withLongOpt(WEBAPP)
        .create(WEBAPP.charAt(0)));
  }

  public void printHelp(PrintWriter out) {
    out.println("Usage: set server");
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    if (args.size() == 1) {
      printHelp(io.out);
      io.out.println();
      return null;
    }

    CommandLine line = parseOptions(this, 1, args);
    if (line.hasOption(HOST)) {
      Environment.setServerHost(line.getOptionValue(HOST));
    }
    if (line.hasOption(PORT)) {
      Environment.setServerPort(line.getOptionValue(PORT));
    }
    if (line.hasOption(WEBAPP)) {
      Environment.setServerWebapp(line.getOptionValue(WEBAPP));
    }

    io.out.println("Server is set successfully.");
    io.out.println();
    return null;
  }
}