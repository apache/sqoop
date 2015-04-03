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
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.sqoop.shell.ShellEnvironment.client;
import static org.apache.sqoop.shell.ShellEnvironment.printlnResource;
import static org.apache.sqoop.shell.ShellEnvironment.resourceString;

@SuppressWarnings("serial")
public class RevokeRoleFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public RevokeRoleFunction() {
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_PRINCIPAL_TYPE)
        .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL_TYPE))
        .isRequired()
        .hasArgs()
        .create()
    );
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_PRINCIPAL)
        .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL))
        .isRequired()
        .hasArgs()
        .create()
    );
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_ROLE)
        .withDescription(resourceString(Constants.RES_PROMPT_ROLE))
        .isRequired()
        .hasArgs()
        .create(Constants.OPT_ROLE_CHAR)
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return revokeRole(
      line.getOptionValue(Constants.OPT_ROLE),
      line.getOptionValue(Constants.OPT_PRINCIPAL_TYPE),
      line.getOptionValue(Constants.OPT_PRINCIPAL));
  }

  private Status revokeRole(String role, String principalType, String principal) throws IOException {
    MRole roleObject = new MRole(role);
    MPrincipal principalObject = new MPrincipal(principal, principalType);

    client.revokeRole(
      Arrays.asList(roleObject),
      Arrays.asList(principalObject));

    client.clearCache();

    printlnResource(Constants.RES_REVOKE_ROLE_SUCCESSFUL,
      role, principalType + " " + principal);

    return Status.OK;
  }
}
