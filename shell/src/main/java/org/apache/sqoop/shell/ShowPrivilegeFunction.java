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
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.shell.utils.TableDisplayer;
import org.apache.sqoop.validation.Status;

import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.client;
import static org.apache.sqoop.shell.ShellEnvironment.resourceString;

@SuppressWarnings("serial")
public class ShowPrivilegeFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public ShowPrivilegeFunction() {
    this.addOption(OptionBuilder
            .withLongOpt(Constants.OPT_PRINCIPAL_TYPE)
            .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL_TYPE))
            .hasArg()
            .isRequired()
            .create()
    );
    this.addOption(OptionBuilder
            .withLongOpt(Constants.OPT_PRINCIPAL)
            .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL))
            .hasArg()
            .isRequired()
            .create()
    );
    this.addOption(OptionBuilder
            .withLongOpt(Constants.OPT_RESOURCE_TYPE)
            .withDescription(resourceString(Constants.RES_PROMPT_RESOURCE_TYPE))
            .hasArg()
            .create()
    );
    this.addOption(OptionBuilder
            .withLongOpt(Constants.OPT_RESOURCE)
            .withDescription(resourceString(Constants.RES_PROMPT_RESOURCE))
            .hasArg()
            .create()
    );
  }

  @Override
  public Object executeFunction(CommandLine line, boolean isInteractive) {
    if (line.hasOption(Constants.OPT_RESOURCE) ^ line.hasOption(Constants.OPT_RESOURCE_TYPE)) {
      throw new SqoopException(ShellError.SHELL_0003,
          ShellEnvironment.getResourceBundle().getString(Constants.RES_SHOW_PRIVILEGE_BAD_ARGUMENTS_RESOURCE_TYPE));
    }

    MPrincipal principal = new MPrincipal(
        line.getOptionValue(Constants.OPT_PRINCIPAL),
        line.getOptionValue(Constants.OPT_PRINCIPAL_TYPE));

    MResource resource = (line.hasOption(Constants.OPT_RESOURCE))
        ? new MResource(line.getOptionValue(Constants.OPT_RESOURCE), line.getOptionValue(Constants.OPT_RESOURCE_TYPE)) : null;

    showPrivileges(principal, resource);

    return Status.OK;
  }

  private void showPrivileges(MPrincipal principal, MResource resource) {
    List<MPrivilege> privileges = client.getPrivilegesByPrincipal(principal, resource);

    List<String> header = new LinkedList<String>();
    header.add(resourceString(Constants.RES_TABLE_HEADER_PRIVILEGE_ACTION));
    header.add(resourceString(Constants.RES_TABLE_HEADER_RESOURCE_NAME));
    header.add(resourceString(Constants.RES_TABLE_HEADER_RESOURCE_TYPE));
    header.add(resourceString(Constants.RES_TABLE_HEADER_PRIVILEGE_WITH_GRANT));

    List<String> actions = new LinkedList<String>();
    List<String> resourceNames = new LinkedList<String>();
    List<String> resourceTypes = new LinkedList<String>();
    List<String> withGrant = new LinkedList<String>();

    for (MPrivilege privilege : privileges) {
      actions.add(privilege.getAction());
      resourceNames.add(privilege.getResource().getName());
      resourceTypes.add(privilege.getResource().getType());
      withGrant.add(Boolean.toString(privilege.isWith_grant_option()));
    }

    TableDisplayer.display(header, actions, resourceNames, resourceTypes, withGrant);
  }
}
