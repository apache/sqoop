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

import org.apache.sqoop.client.core.ClientError;
import org.apache.sqoop.common.SqoopException;
import org.codehaus.groovy.tools.shell.Shell;

import java.util.List;

/**
 *
 */
public class UpdateCommand extends SqoopCommand {

  private UpdateConnectionFunction connectionFunction;
  private UpdateJobFunction jobFunction;

  public UpdateCommand(Shell shell) {
    super(shell, "update", "\\up",
      new String[] {"connection", "job"},
      "Update", "info");
  }

  public Object execute(List args) {
    if (args.size() == 0) {
      io.out.println("Usage: create " + getUsage());
      io.out.println();
      return null;
    }

    String func = (String)args.get(0);
    if (func.equals("connection")) {
      if (connectionFunction == null) {
        connectionFunction = new UpdateConnectionFunction(io);
      }
      return connectionFunction.execute(args);
    } else if (func.equals("job")) {
      if (jobFunction == null) {
        jobFunction = new UpdateJobFunction(io);
      }
      return jobFunction.execute(args);
    } else {
      String msg = "Usage: update " + getUsage();
      throw new SqoopException(ClientError.CLIENT_0002, msg);
    }
  }
}
