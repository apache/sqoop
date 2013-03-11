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
 *
 */
public class SetOptionFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  protected SetOptionFunction() {
    this.addOption(OptionBuilder.hasArg()
      .withDescription(resourceString(Constants.RES_SET_PROMPT_OPT_NAME))
      .withLongOpt(Constants.OPT_NAME)
      .create(Constants.OPT_NAME_CHAR));
    this.addOption(OptionBuilder.hasArg()
      .withDescription(resourceString(Constants.RES_SET_PROMPT_OPT_VALUE))
      .withLongOpt(Constants.OPT_VALUE)
      .create(Constants.OPT_VALUE_CHAR));
  }

  public Object executeFunction(CommandLine line) {
    if (!line.hasOption(Constants.OPT_NAME)) {
      printlnResource(Constants.RES_ARGS_NAME_MISSING);
      return null;
    }
    if (!line.hasOption(Constants.OPT_VALUE)) {
      printlnResource(Constants.RES_ARGS_VALUE_MISSING);
      return null;
    }

    handleOptionSetting(line.getOptionValue(Constants.OPT_NAME), line.getOptionValue(Constants.OPT_VALUE));

    return null;
  }

  private void handleOptionSetting(String name, String value) {
    if(name.equals(Constants.OPT_VERBOSE)) {
      boolean newValue = false;

      if(value.equals("1") || value.equals("true")) {
        newValue = true;
      }

      setVerbose(newValue);
      printlnResource(Constants.RES_SET_VERBOSE_CHANGED, newValue);
      return;
    }

    printlnResource(Constants.RES_SET_UNKNOWN_OPT_IGNORED, name);
  }
}
