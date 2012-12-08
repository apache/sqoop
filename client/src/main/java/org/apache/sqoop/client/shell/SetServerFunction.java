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
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.client.core.Environment;
import org.codehaus.groovy.tools.shell.IO;

@SuppressWarnings("serial")
public class SetServerFunction extends SqoopFunction
{


  private IO io;


  @SuppressWarnings("static-access")
  protected SetServerFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_HOST)
        .withDescription(getResource().getString(Constants.RES_SET_HOST_DESCRIPTION))
        .withLongOpt(Constants.OPT_HOST)
        .create(Constants.OPT_HOST_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_PORT)
        .withDescription(getResource().getString(Constants.RES_SET_PORT_DESCRIPTION))
        .withLongOpt(Constants.OPT_PORT)
        .create(Constants.OPT_PORT_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_WEBAPP)
        .withDescription(getResource().getString(Constants.RES_WEBAPP_DESCRIPTION))
        .withLongOpt(Constants.OPT_WEBAPP)
        .create(Constants.OPT_WEBAPP_CHAR));
  }

  public void printHelp(PrintWriter out) {
    out.println(getResource().getString(Constants.RES_SET_SERVER_USAGE));
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    if (args.size() == 1) {
      printHelp(io.out);
      io.out.println();
      return null;
    }

    CommandLine line = parseOptions(this, 1, args);
    if (line.hasOption(Constants.OPT_HOST)) {
      Environment.setServerHost(line.getOptionValue(Constants.OPT_HOST));
    }
    if (line.hasOption(Constants.OPT_PORT)) {
      Environment.setServerPort(line.getOptionValue(Constants.OPT_PORT));
    }
    if (line.hasOption(Constants.OPT_WEBAPP)) {
      Environment.setServerWebapp(line.getOptionValue(Constants.OPT_WEBAPP));
    }

    io.out.println(getResource().getString(Constants.RES_SET_SERVER_SUCCESSFUL));
    io.out.println();
    return null;
  }
}