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

/**
 * Show client internal options
 */
public class ShowOptionFunction extends SqoopFunction {
  /**
   * Construct new object.
   */
  @SuppressWarnings("static-access")
  protected ShowOptionFunction() {
    this.addOption(OptionBuilder
        .hasArg().withArgName(Constants.OPT_NAME)
        .withDescription(resource.getString(Constants.RES_SET_PROMPT_OPT_NAME))
        .withLongOpt(Constants.OPT_NAME)
        .create(Constants.OPT_NAME_CHAR));
  }

  /**
   * Execute this function from parsed command line.
   */
  public Object executeFunction(CommandLine line) {
    if (line.getArgs().length == 1) {
      printAllOptions();
      return null;
    }

    if (line.hasOption(Constants.OPT_NAME)) {
      String optionName = line.getOptionValue(Constants.OPT_NAME);

      if(optionName.equals(Constants.OPT_VERBOSE)) {
        printVerbose();
      }
    }

    return null;
  }

  /**
   * Print all known client options.
   */
  private void printAllOptions() {
    printVerbose();
  }

  /**
   * Print verbose option.
   */
  private void printVerbose() {
    print("Verbose = ");
    println(String.valueOf(isVerboose()));
  }
}
