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
import org.apache.sqoop.client.core.Constants;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

@SuppressWarnings("serial")
public class SetServerFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  protected SetServerFunction() {
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_HOST)
        .withDescription(resourceString(Constants.RES_SET_HOST_DESCRIPTION))
        .withLongOpt(Constants.OPT_HOST)
        .create(Constants.OPT_HOST_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_PORT)
        .withDescription(resourceString(Constants.RES_SET_PORT_DESCRIPTION))
        .withLongOpt(Constants.OPT_PORT)
        .create(Constants.OPT_PORT_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_WEBAPP)
        .withDescription(resourceString(Constants.RES_WEBAPP_DESCRIPTION))
        .withLongOpt(Constants.OPT_WEBAPP)
        .create(Constants.OPT_WEBAPP_CHAR));
  }

  public Object executeFunction(CommandLine line) {
    if (line.getArgs().length == 1) {
      printlnResource(Constants.RES_SET_SERVER_USAGE);
      return null;
    }

    if (line.hasOption(Constants.OPT_HOST)) {
      setServerHost(line.getOptionValue(Constants.OPT_HOST));
    }
    if (line.hasOption(Constants.OPT_PORT)) {
      setServerPort(line.getOptionValue(Constants.OPT_PORT));
    }
    if (line.hasOption(Constants.OPT_WEBAPP)) {
      setServerWebapp(line.getOptionValue(Constants.OPT_WEBAPP));
    }

    printlnResource(Constants.RES_SET_SERVER_SUCCESSFUL);
    return null;
  }
}
