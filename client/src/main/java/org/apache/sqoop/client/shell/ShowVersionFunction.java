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

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.client.request.VersionRequest;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.json.VersionBean;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

@SuppressWarnings("serial")
public class ShowVersionFunction extends SqoopFunction {
  private VersionRequest versionRequest;


  @SuppressWarnings("static-access")
  protected ShowVersionFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_VERSIONS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_VERSION_SERVER))
        .withLongOpt(Constants.OPT_SERVER)
        .create(Constants.OPT_SERVER_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_VERSION_CLIENT))
        .withLongOpt(Constants.OPT_CLIENT)
        .create(Constants.OPT_CLIENT_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_VERSION_PROTOCOL))
        .withLongOpt(Constants.OPT_PROTOCOL)
        .create(Constants.OPT_PROTOCOL_CHAR));
  }

  public Object executeFunction(CommandLine line) {
    if (line.getArgs().length == 1) {
      printlnResource(Constants.RES_SHOW_VERSION_USAGE);
      return null;
    }

    if (line.hasOption(Constants.OPT_ALL)) {
      showVersion(true, true, true);

    } else {
      boolean server = false, client = false, protocol = false;
      if (line.hasOption(Constants.OPT_SERVER)) {
        server = true;
      }
      if (line.hasOption(Constants.OPT_CLIENT)) {
        client = true;
      }
      if (line.hasOption(Constants.OPT_PROTOCOL)) {
        protocol = true;
      }

      showVersion(server, client, protocol);
    }

    return null;
  }

  private void showVersion(boolean server, boolean client, boolean protocol) {

    // Print out client string if needed
    if (client) {
      printlnResource(Constants.RES_SHOW_PROMPT_VERSION_CLIENT_SERVER,
        Constants.OPT_CLIENT,
        VersionInfo.getVersion(),
        VersionInfo.getRevision(),
        VersionInfo.getUser(),
        VersionInfo.getDate()
      );
    }

    // If only client version was required we do not need to continue
    if(!server && !protocol) {
      return;
    }

    if (versionRequest == null) {
      versionRequest = new VersionRequest();
    }
    VersionBean versionBean = versionRequest.doGet(getServerUrl());

    if (server) {
      printlnResource(Constants.RES_SHOW_PROMPT_VERSION_CLIENT_SERVER,
        Constants.OPT_SERVER,
        versionBean.getVersion(),
        versionBean.getRevision(),
        versionBean.getUser(),
        versionBean.getDate()
      );
    }

    if (protocol) {
      printlnResource(Constants.RES_SHOW_PROMPT_VERSION_PROTOCOL,
        Arrays.toString(versionBean.getProtocols())
      );
    }
  }
}
