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

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.request.VersionResourceRequest;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.json.VersionBean;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.validation.Status;

import static org.apache.sqoop.shell.ShellEnvironment.*;

@SuppressWarnings("serial")
public class ShowVersionFunction extends SqoopFunction {
  private VersionResourceRequest versionRequest;


  @SuppressWarnings("static-access")
  public ShowVersionFunction() {
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_ALL_VERSIONS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_SERVER_VERSION))
        .withLongOpt(Constants.OPT_SERVER)
        .create(Constants.OPT_SERVER_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_CLIENT_VERSION))
        .withLongOpt(Constants.OPT_CLIENT)
        .create(Constants.OPT_CLIENT_CHAR));
    this.addOption(OptionBuilder
        .withDescription(resourceString(Constants.RES_SHOW_PROMPT_DISPLAY_REST_API_VERSION))
        .withLongOpt(Constants.OPT_REST_API)
        .create(Constants.OPT_API_CHAR));
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.getArgs().length == 1) {
      printlnResource(Constants.RES_SHOW_VERSION_USAGE);
      return null;
    }

    if (line.hasOption(Constants.OPT_ALL)) {
      showVersion(true, true, true);

    } else {
      boolean server = false, client = false, restApi = false;
      if (line.hasOption(Constants.OPT_SERVER)) {
        server = true;
      }
      if (line.hasOption(Constants.OPT_CLIENT)) {
        client = true;
      }
      if (line.hasOption(Constants.OPT_REST_API)) {
        restApi = true;
      }

      showVersion(server, client, restApi);
    }

    return Status.OK;
  }

  private void showVersion(boolean server, boolean client, boolean restApi) {

    // If no option has been given, print out client version as default
    if (!client && !server && !restApi) {
      client = true;
    }

    // Print out client string if needed
    if (client) {
      printlnResource(Constants.RES_SHOW_PROMPT_VERSION_CLIENT_SERVER,
        Constants.OPT_CLIENT,
        // See SQOOP-1623 to understand how the client version is derived.
        VersionInfo.getBuildVersion(),
        VersionInfo.getSourceRevision(),
        VersionInfo.getUser(),
        VersionInfo.getBuildDate()
      );
    }

    // If only client version was required we do not need to continue
    if(!server && !restApi) {
      return;
    }

    if (versionRequest == null) {
      versionRequest = new VersionResourceRequest();
    }
    VersionBean versionBean = versionRequest.read(getServerUrl());

    if (server) {
      printlnResource(Constants.RES_SHOW_PROMPT_VERSION_CLIENT_SERVER,
        Constants.OPT_SERVER,
        versionBean.getBuildVersion(),
        versionBean.getSourceRevision(),
        versionBean.getSystemUser(),
        versionBean.getBuildDate()
      );
    }

    if (restApi) {
      printlnResource(Constants.RES_SHOW_PROMPT_API_VERSIONS,
        Arrays.toString(versionBean.getSupportedAPIVersions())
      );
    }
  }
}
