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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.client.request.VersionRequest;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.json.VersionBean;
import org.codehaus.groovy.tools.shell.IO;

@SuppressWarnings("serial")
public class ShowVersionFunction extends SqoopFunction
{
  public static final String ALL = "all";
  public static final String SERVER = "server";
  public static final String CLIENT = "client";
  public static final String PROTOCOL = "protocol";

  private IO io;
  private VersionRequest versionRequest;

  @SuppressWarnings("static-access")
  protected ShowVersionFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
        .withDescription("Display all versions")
        .withLongOpt(ALL)
        .create(ALL.charAt(0)));
    this.addOption(OptionBuilder
        .withDescription("Display server version")
        .withLongOpt(SERVER)
        .create(SERVER.charAt(0)));
    this.addOption(OptionBuilder
        .withDescription("Display client version")
        .withLongOpt(CLIENT)
        .create(CLIENT.charAt(0)));
    this.addOption(OptionBuilder
        .withDescription("Display protocol version")
        .withLongOpt(PROTOCOL)
        .create(PROTOCOL.charAt(0)));
  }

  public void printHelp(PrintWriter out) {
    out.println("Usage: show version");
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
      showVersion(true, true, true);

    } else {
      boolean server = false, client = false, protocol = false;
      if (line.hasOption(SERVER)) {
        server = true;
      }
      if (line.hasOption(CLIENT)) {
        client = true;
      }
      if (line.hasOption(PROTOCOL)) {
        protocol = true;
      }

      showVersion(server, client, protocol);
    }

    return null;
  }

  private void showVersion(boolean server, boolean client, boolean protocol) {
    if (versionRequest == null) {
      versionRequest = new VersionRequest();
    }
    VersionBean versionBean =
        versionRequest.version(Environment.getServerUrl());

    if (server) {
      io.out.println("@|bold Server version:|@");
      io.out.print("  Sqoop ");
      io.out.print(versionBean.getVersion());
      io.out.print(" revision ");
      io.out.println(versionBean.getRevision());
      io.out.print("  Compiled by ");
      io.out.print(versionBean.getUser());
      io.out.print(" on ");
      io.out.println(versionBean.getDate());
    }

    if (client) {
      io.out.println("@|bold Client version:|@");
      io.out.print("  Sqoop ");
      io.out.print(VersionInfo.getVersion());
      io.out.print(" revision ");
      io.out.println(VersionInfo.getRevision());
      io.out.print("  Compiled by ");
      io.out.print(VersionInfo.getUser());
      io.out.print(" on ");
      io.out.println(VersionInfo.getDate());
    }

    if (protocol) {
      io.out.println("@|bold Protocol version:|@");
      io.out.print("  ");
      io.out.println(Arrays.toString(versionBean.getProtocols()));
    }

    io.out.println();
  }
}