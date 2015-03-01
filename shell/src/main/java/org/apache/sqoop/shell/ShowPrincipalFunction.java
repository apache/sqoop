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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.client;
import static org.apache.sqoop.shell.ShellEnvironment.resourceString;

@SuppressWarnings("serial")
public class ShowPrincipalFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public ShowPrincipalFunction() {
    this.addOption(OptionBuilder
            .withLongOpt(Constants.OPT_ROLE)
            .withDescription(resourceString(Constants.RES_PROMPT_ROLE))
            .hasArg()
            .isRequired()
            .create(Constants.OPT_ROLE_CHAR)
    );
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    MRole role = null;

    if (line.hasOption(Constants.OPT_ROLE)) {
      role = new MRole(line.getOptionValue(Constants.OPT_ROLE));
    }

    showPrincipals(role);

    return Status.OK;
  }

  private void showPrincipals(MRole role) {
    List<MPrincipal> principals = client.getPrincipalsByRole(role);

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_PRINCIPAL_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_PRINCIPAL_TYPE));

    List<String> names = new LinkedList<String>();
    List<String> types = new LinkedList<String>();

    for (MPrincipal principal : principals) {
      names.add(principal.getName());
      types.add(principal.getType());
    }

    TableDisplayer.display(header, names, types);
  }
}
