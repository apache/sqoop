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
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.client;
import static org.apache.sqoop.shell.ShellEnvironment.resourceString;

@SuppressWarnings("serial")
public class ShowRoleFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public ShowRoleFunction() {
    this.addOption(OptionBuilder
            .withLongOpt(Constants.OPT_PRINCIPAL_TYPE)
            .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL_TYPE))
            .hasArg()
            .create()
    );
    this.addOption(OptionBuilder
            .withLongOpt(Constants.OPT_PRINCIPAL)
            .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL))
            .hasArg()
            .create()
    );
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_PRINCIPAL) ^ line.hasOption(Constants.OPT_PRINCIPAL_TYPE)) {
      throw new SqoopException(ShellError.SHELL_0003,
          ShellEnvironment.getResourceBundle().getString(Constants.RES_SHOW_ROLE_BAD_ARGUMENTS_PRINCIPAL_TYPE));
    }

    MPrincipal principal = (line.hasOption(Constants.OPT_PRINCIPAL))
        ? new MPrincipal(
        line.getOptionValue(Constants.OPT_PRINCIPAL),
        line.getOptionValue(Constants.OPT_PRINCIPAL_TYPE))
        : null;

    showRoles(principal);

    return Status.OK;
  }

  private void showRoles(MPrincipal principal) {
    List<MRole> roles = (principal == null) ? client.getRoles() : client.getRolesByPrincipal(principal);

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_ROLE_NAME));

    List<String> names = new LinkedList<String>();

    for (MRole role : roles) {
      names.add(role.getName());
    }

    TableDisplayer.display(header, names);
  }
}
