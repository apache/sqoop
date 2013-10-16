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

/**
 * Handles enabling of a connection object
 */
@SuppressWarnings("serial")
public class DisableConnectionFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public DisableConnectionFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_CONN_ID))
      .withLongOpt(Constants.OPT_XID)
      .hasArg()
      .create('x'));
  }

  @Override
  public boolean validateArgs(CommandLine line) {
    if (!line.hasOption(Constants.OPT_XID)) {
      printlnResource(Constants.RES_ARGS_XID_MISSING);
      return false;
    }
    return true;
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    client.enableConnection(getLong(line, Constants.OPT_XID), false);
    return Status.FINE;
  }
}
