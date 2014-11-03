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
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.validation.Status;

import static org.apache.sqoop.shell.ShellEnvironment.*;

@SuppressWarnings("serial")
public class ShowServerFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public ShowServerFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_SERVERS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_SERVER_HOST))
        .withLongOpt(Constants.OPT_HOST)
        .create(Constants.OPT_HOST_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_SERVER_PORT))
        .withLongOpt(Constants.OPT_PORT)
        .create(Constants.OPT_PORT_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_SERVER_WEBAPP))
        .withLongOpt(Constants.OPT_WEBAPP)
        .create(Constants.OPT_WEBAPP_CHAR));
  }

  @Override
  public boolean validateArgs(CommandLine line) {
    if (line.getArgs().length == 1) {
      printlnResource(Constants.RES_SHOW_SERVER_USAGE);
      return false;
    }
    return true;
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_ALL)) {
      showServer(true, true, true, true);

    } else {
      boolean host = false, port = false, webapp = false, version = false;
      if (line.hasOption(Constants.OPT_HOST)) {
        host = true;
      }
      if (line.hasOption(Constants.OPT_PORT)) {
        port = true;
      }
      if (line.hasOption(Constants.OPT_WEBAPP)) {
        webapp = true;
      }

      showServer(host, port, webapp, version);
    }

    return Status.OK;
  }

  private void showServer(boolean host, boolean port, boolean webapp, boolean version) {
    if (host) {
      printlnResource(Constants.RES_SHOW_PROMPT_SERVER_HOST, getServerHost());
    }

    if (port) {
      printlnResource(Constants.RES_SHOW_PROMPT_SERVER_PORT, getServerPort());
    }

    if (webapp) {
      printlnResource(Constants.RES_SHOW_PROMPT_SERVER_WEBAPP, getServerWebapp());
    }
  }
}
