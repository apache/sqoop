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

import java.util.List;
import org.apache.sqoop.client.core.Constants;
import org.codehaus.groovy.tools.shell.Shell;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

public class ShowCommand extends SqoopCommand
{
  private ShowServerFunction serverFunction;
  private ShowVersionFunction versionFunction;
  private ShowConnectorFunction connectorFunction;
  private ShowJobFunction jobFunction;
  private ShowFrameworkFunction frameworkFunction;
  private ShowConnectionFunction connectionFunction;
  private ShowOptionFunction optionFunction;


  protected ShowCommand(Shell shell) {
    super(shell, Constants.CMD_SHOW, Constants.CMD_SHOW_SC,
        new String[] {Constants.FN_SERVER, Constants.FN_VERSION,
          Constants.FN_CONNECTOR, Constants.FN_FRAMEWORK,
          Constants.FN_CONNECTION, Constants.FN_JOB, Constants.FN_OPTION },
          Constants.PRE_SHOW, Constants.SUF_INFO);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Object executeCommand(List args) {
    if (args.size() == 0) {
      printlnResource(Constants.RES_SHOW_USAGE, getUsage());
      return null;
    }

    String func = (String)args.get(0);
    if (func.equals(Constants.FN_SERVER)) {
      if (serverFunction == null) {
        serverFunction = new ShowServerFunction();
      }
      return serverFunction.execute(args);

    } else if (func.equals(Constants.FN_VERSION)) {
      if (versionFunction == null) {
        versionFunction = new ShowVersionFunction();
      }
      return versionFunction.execute(args);

    } else if (func.equals(Constants.FN_CONNECTOR)) {
      if (connectorFunction == null) {
        connectorFunction = new ShowConnectorFunction();
      }
      return connectorFunction.execute(args);

    } else if (func.equals(Constants.FN_FRAMEWORK)) {
      if (frameworkFunction == null) {
        frameworkFunction = new ShowFrameworkFunction();
      }
      return frameworkFunction.execute(args);

    } else if (func.equals(Constants.FN_CONNECTION)) {
      if (connectionFunction == null) {
        connectionFunction = new ShowConnectionFunction();
      }
      return connectionFunction.execute(args);

    } else if (func.equals(Constants.FN_JOB)) {
      if (jobFunction == null) {
        jobFunction = new ShowJobFunction();
      }
      return jobFunction.execute(args);
    } else if (func.equals(Constants.FN_OPTION)) {
      if (optionFunction == null) {
        optionFunction = new ShowOptionFunction();
      }
      return optionFunction.execute(args);
    } else {
      printlnResource(Constants.RES_FUNCTION_UNKNOWN, func);
      return null;
    }
  }
}
