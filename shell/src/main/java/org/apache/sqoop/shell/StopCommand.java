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

import java.util.List;

import org.apache.sqoop.shell.core.Constants;
import org.codehaus.groovy.tools.shell.Shell;

import static org.apache.sqoop.shell.ShellEnvironment.printlnResource;

public class StopCommand extends SqoopCommand {

  private StopJobFunction stopJobFunction;

  protected StopCommand(Shell shell) {
    super(shell, Constants.CMD_STOP, Constants.CMD_STOP_SC,
        new String[] { Constants.FN_JOB }, Constants.PRE_STOP, null);
  }
  @Override
  public Object executeCommand(List args) {
    if (args.size() == 0) {
      printlnResource(Constants.RES_STOP_USAGE, getUsage());
      return null;
    }

    String func = (String) args.get(0);
    if (func.equals(Constants.FN_JOB)) {
      if (stopJobFunction == null) {
        stopJobFunction = new StopJobFunction();
      }
      return stopJobFunction.execute(args);
    } else {
      printlnResource(Constants.RES_FUNCTION_UNKNOWN, func);
    }
    return null;
  }
}
