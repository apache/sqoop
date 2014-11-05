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

import static org.apache.sqoop.shell.ShellEnvironment.client;
import static org.apache.sqoop.shell.ShellEnvironment.printlnResource;
import static org.apache.sqoop.shell.utils.ConfigDisplayer.displayDriverConfigDetails;

import org.apache.commons.cli.CommandLine;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.validation.Status;

/**
 *
 */
@SuppressWarnings("serial")
public class ShowDriverFunction extends SqoopFunction {
  public ShowDriverFunction() {
  }

  @Override
  public boolean validateArgs(CommandLine line) {
    if (line.getArgs().length != 0) {
      printlnResource(Constants.RES_SHOW_DRIVER_USAGE);
      return false;
    }
    return true;
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    showDriver();
    return Status.OK;
  }

  private void showDriver() {
    printlnResource(Constants.RES_SHOW_PROMPT_DRIVER_OPTS, client.getDriver().getPersistenceId());
    displayDriverConfigDetails(client.getDriverConfig(), client.getDriverConfigBundle());
  }
}
