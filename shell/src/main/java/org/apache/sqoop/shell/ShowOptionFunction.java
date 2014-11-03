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
 * Show client internal options
 */
@SuppressWarnings("serial")
public class ShowOptionFunction extends SqoopFunction {
  /**
   * Construct new object.
   */
  @SuppressWarnings("static-access")
  public ShowOptionFunction() {
    this.addOption(OptionBuilder
        .hasArg().withArgName(Constants.OPT_NAME)
        .withDescription(resource.getString(Constants.RES_SET_PROMPT_OPT_NAME))
        .withLongOpt(Constants.OPT_NAME)
        .create(Constants.OPT_NAME_CHAR));
  }

  @Override
  public boolean validateArgs(CommandLine line) {
    if (line.getArgs().length == 1) {
      printAllOptions();
      return false;
    }
    return true;
  }

  /**
   * Execute this function from parsed command line.
   */
  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_NAME)) {
      String optionName = line.getOptionValue(Constants.OPT_NAME);

      if(optionName.equals(Constants.OPT_VERBOSE)) {
        printVerbose();
      }

      if(optionName.equals(Constants.OPT_POLL_TIMEOUT)) {
        printPollTimeout();
      }
    } else {
      printAllOptions();
      return null;
    }

    return Status.OK;
  }

  /**
   * Print all known client options.
   */
  private void printAllOptions() {
    printVerbose();
    printPollTimeout();
  }

  /**
   * Print verbose option.
   */
  private void printVerbose() {
    print("Verbose = ");
    println(String.valueOf(isVerbose()));
  }

  /**
   * Print poll-timeout option.
   */
  private void printPollTimeout() {
    print("Poll-timeout = ");
    println(String.valueOf(getPollTimeout()));
  }
}
