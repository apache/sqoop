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

import org.apache.sqoop.client.core.ClientError;
import org.apache.sqoop.common.SqoopException;
import org.codehaus.groovy.tools.shell.Shell;

public class ShowCommand extends SqoopCommand
{
  private ShowServerFunction serverFunction;
  private ShowVersionFunction versionFunction;
  private ShowConnectorFunction connectorFunction;

  protected ShowCommand(Shell shell) {
    super(shell, "show", "\\sh",
        new String[] {"server", "version", "connector", "connection", "job"},
        "Show", "info");
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Object execute(List args) {
    if (args.size() == 0) {
      shell.getIo().out.println("Usage: show " + getUsage());
      io.out.println();
      return null;
    }

    String func = (String)args.get(0);
    if (func.equals("server")) {
      if (serverFunction == null) {
        serverFunction = new ShowServerFunction(io);
      }
      return serverFunction.execute(args);

    } else if (func.equals("version")) {
      if (versionFunction == null) {
        versionFunction = new ShowVersionFunction(io);
      }
      return versionFunction.execute(args);

    } else if (func.equals("connector")) {
      return null;

    } else if (func.equals("connection")) {
      return null;

    } else if (func.equals("job")) {
      return null;

    } else {
      String msg = "Usage: show " + getUsage();
      throw new SqoopException(ClientError.CLIENT_0002, msg);
    }
  }
}