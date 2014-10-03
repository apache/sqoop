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

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.shell.utils.ConfigOptions;

import static org.apache.sqoop.shell.ShellEnvironment.*;

@SuppressWarnings("serial")
abstract public class SqoopFunction extends Options {

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printOptions(getIo().out, formatter.getWidth(), this, 0, 4);
  }

  public abstract Object executeFunction(CommandLine line, boolean isInteractive) throws IOException;

  public boolean validateArgs(CommandLine line) {
    return true;
  }

  public Object execute(List<String> args) {
    CommandLine line = ConfigOptions.parseOptions(this, 1, args, true);

    try {
      if (validateArgs(line)) {
        return executeFunction(line, isInteractive());
      } else {
        return null;
      }
    } catch (IOException ex) {
      throw new SqoopException(ShellError.SHELL_0005, ex);
    }
  }

  protected long getLong(CommandLine line, String parameterName) {
    return Long.parseLong(line.getOptionValue(parameterName));
  }
}
