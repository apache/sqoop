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

import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.client.request.FrameworkRequest;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJobForms;
import org.codehaus.groovy.tools.shell.IO;

import java.io.PrintWriter;
import java.util.List;

import static org.apache.sqoop.client.display.FormDisplayer.*;

/**
 *
 */
public class ShowFrameworkFunction extends SqoopFunction {

  private IO io;
  private FrameworkRequest frameworkRequest;

  @SuppressWarnings("static-access")
  protected ShowFrameworkFunction(IO io) {
    this.io = io;
  }

  public void printHelp(PrintWriter out) {
    out.println("Usage: show framework");
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    if (args.size() != 1) {
      printHelp(io.out);
      io.out.println();
      return null;
    }

    showFramework();

    return null;
  }

  private void showFramework() {
    if (frameworkRequest == null) {
      frameworkRequest = new FrameworkRequest();
    }

    FrameworkBean frameworkBean =
      frameworkRequest.doGet(Environment.getServerUrl());
    MFramework framework = frameworkBean.getFramework();

    io.out.println("@|bold Framework specific options: |@");

    io.out.print("Persistent id: ");
    io.out.println(framework.getPersistenceId());

    displayFormDetails(io, framework);

    io.out.println();
  }
}
