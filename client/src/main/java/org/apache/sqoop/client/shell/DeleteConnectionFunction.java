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
import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.client.request.ConnectionRequest;
import org.codehaus.groovy.tools.shell.IO;

import java.util.List;

/**
 *
 */
public class DeleteConnectionFunction extends SqoopFunction {

  private IO io;

  private ConnectionRequest connectionRequest;

  private static final String XID = "xid";

  @SuppressWarnings("static-access")
  public DeleteConnectionFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
      .withDescription("Connection ID")
      .withLongOpt(XID)
      .hasArg()
      .create('x'));
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (!line.hasOption(XID)) {
      io.out.println("Required argument --xid is missing.");
      return null;
    }

    if (connectionRequest == null) {
      connectionRequest = new ConnectionRequest();
    }

    connectionRequest.delete(Environment.getServerUrl(),
                             Long.valueOf(line.getOptionValue(XID)));

    return null;
  }

}
