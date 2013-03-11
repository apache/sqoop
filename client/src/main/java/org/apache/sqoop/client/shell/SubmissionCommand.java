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

import org.apache.sqoop.client.core.Constants;
import org.codehaus.groovy.tools.shell.Shell;

import java.text.MessageFormat;
import java.util.List;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

/**
 *
 */
public class SubmissionCommand  extends SqoopCommand {

  private SubmissionStartFunction startFunction;
  private SubmissionStopFunction stopFunction;
  private SubmissionStatusFunction statusFunction;

  public SubmissionCommand(Shell shell) {
    super(shell, Constants.CMD_SUBMISSION, Constants.CMD_SUBMISSION_SC,
      new String[] {Constants.FN_START, Constants.FN_STOP,
          Constants.FN_STATUS},
      Constants.PRE_SUBMISSION, Constants.SUF_INFO);
  }

  public Object executeCommand(List args) {
    String usageMsg = MessageFormat.format(resource.getString(Constants.RES_SUBMISSION_USAGE), getUsage());
    if (args.size() == 0) {
      println(usageMsg);
      return null;
    }

    String func = (String)args.get(0);
    if (func.equals(Constants.FN_START)) {
      if (startFunction == null) {
        startFunction = new SubmissionStartFunction();
      }
      return startFunction.execute(args);
    } else if (func.equals(Constants.FN_STOP)) {
        if (stopFunction == null) {
          stopFunction = new SubmissionStopFunction();
        }
        return stopFunction.execute(args);
    } else if (func.equals(Constants.FN_STATUS)) {
      if (statusFunction == null) {
        statusFunction = new SubmissionStatusFunction();
      }
      return statusFunction.execute(args);
    } else {
      printlnResource(Constants.RES_FUNCTION_UNKNOWN, func);
      return null;
    }
  }
}
