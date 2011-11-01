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

package org.apache.sqoop.tool;

import java.util.Set;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.cli.ToolOptions;

/**
 * Tool that explains the usage of Sqoop.
 */
public class HelpTool extends com.cloudera.sqoop.tool.BaseSqoopTool {

  public HelpTool() {
    super("help");
  }

  /**
   * @param str the string to right-side pad
   * @param num the minimum number of characters to return
   * @return 'str' with enough right padding to make it num characters long.
   */
  private static String padRight(String str, int num) {
    StringBuilder sb = new StringBuilder();
    sb.append(str);
    for (int count = str.length(); count < num; count++) {
      sb.append(" ");
    }

    return sb.toString();
  }

  /**
   * Print out a list of all SqoopTool implementations and their
   * descriptions.
   */
  private void printAvailableTools() {
    System.out.println("usage: sqoop COMMAND [ARGS]");
    System.out.println("");
    System.out.println("Available commands:");

    Set<String> toolNames = getToolNames();

    int maxWidth = 0;
    for (String tool : toolNames) {
      maxWidth = Math.max(maxWidth, tool.length());
    }

    for (String tool : toolNames) {
      System.out.println("  " + padRight(tool, maxWidth+2)
          + getToolDescription(tool));
    }

    System.out.println("");
    System.out.println(
        "See 'sqoop help COMMAND' for information on a specific command.");
  }


  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {

    if (this.extraArguments != null && this.extraArguments.length > 0) {
      if (hasUnrecognizedArgs(extraArguments, 1, extraArguments.length)) {
        return 1;
      }

      SqoopTool subTool = SqoopTool.getTool(extraArguments[0]);
      if (null == subTool) {
        System.out.println("No such tool: " + extraArguments[0]);
        System.out.println(
            "Try 'sqoop help' for a list of available commands.");
        return 1;
      } else {
        ToolOptions toolOpts = new ToolOptions();
        subTool.configureOptions(toolOpts);
        subTool.printHelp(toolOpts);
        return 0;
      }
    } else {
      printAvailableTools();
    }

    return 0;
  }

  @Override
  public void printHelp(ToolOptions opts) {
    System.out.println("usage: sqoop " + getToolName() + " [COMMAND]");
  }
}

