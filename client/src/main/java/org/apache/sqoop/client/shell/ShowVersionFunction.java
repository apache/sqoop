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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.client.request.VersionRequest;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.json.VersionBean;
import org.codehaus.groovy.tools.shell.IO;

@SuppressWarnings("serial")
public class ShowVersionFunction extends SqoopFunction
{


  private IO io;
  private VersionRequest versionRequest;


  @SuppressWarnings("static-access")
  protected ShowVersionFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_ALL_VERSIONS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_VERSION_SERVER))
        .withLongOpt(Constants.OPT_SERVER)
        .create(Constants.OPT_SERVER_CHAR));
    this.addOption(OptionBuilder
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_VERSION_CLIENT))
        .withLongOpt(Constants.OPT_CLIENT)
        .create(Constants.OPT_CLIENT_CHAR));
    this.addOption(OptionBuilder
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_VERSION_PROTOCOL))
        .withLongOpt(Constants.OPT_PROTOCOL)
        .create(Constants.OPT_PROTOCOL_CHAR));
  }

  public void printHelp(PrintWriter out) {
    out.println(getResource().getString(Constants.RES_SHOW_VERSION_USAGE));
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    if (args.size() == 1) {
      printHelp(io.out);
      io.out.println();
      return null;
    }

    CommandLine line = parseOptions(this, 1, args);
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
    String s;
    if (client) {
      s = MessageFormat.format(
        getResource().getString(Constants.RES_SHOW_PROMPT_VERSION_CLIENT_SERVER),
        Constants.OPT_CLIENT,
        VersionInfo.getVersion(),
        VersionInfo.getRevision(),
        VersionInfo.getUser(),
        VersionInfo.getDate()
      );
      io.out.println(StringEscapeUtils.unescapeJava(s));
    }

    // If only client version was required we do not need to continue
    if(!server && !protocol) {
      return;
    }

    if (versionRequest == null) {
      versionRequest = new VersionRequest();
    }
    VersionBean versionBean =
        versionRequest.doGet(Environment.getServerUrl());

    if (server) {
      s = MessageFormat.format(
        getResource().getString(Constants.RES_SHOW_PROMPT_VERSION_CLIENT_SERVER),
        Constants.OPT_SERVER,
        versionBean.getVersion(),
        versionBean.getRevision(),
        versionBean.getUser(),
        versionBean.getDate()
      );
      io.out.println(StringEscapeUtils.unescapeJava(s));
    }

    if (protocol) {
      s = MessageFormat.format(
        getResource().getString(Constants.RES_SHOW_PROMPT_VERSION_PROTOCOL),
        Arrays.toString(versionBean.getProtocols())
      );
      io.out.println(StringEscapeUtils.unescapeJava(s));
    }

    io.out.println();
  }
}
