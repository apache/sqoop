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
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;

import static org.apache.sqoop.shell.ShellEnvironment.*;

public class GrantPrivilegeFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public GrantPrivilegeFunction() {
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_RESOURCE_TYPE)
        .withDescription(resourceString(Constants.RES_PROMPT_RESOURCE_TYPE))
        .isRequired()
        .hasArg()
        .create()
    );
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_RESOURCE)
        .withDescription(resourceString(Constants.RES_PROMPT_RESOURCE))
        .isRequired()
        .hasArg()
        .create()
    );
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_ACTION)
        .withDescription(resourceString(Constants.RES_PROMPT_ACTION))
        .isRequired()
        .hasArg()
        .create(Constants.OPT_ACTION_CHAR)
    );
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_PRINCIPAL)
        .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL))
        .isRequired()
        .hasArg()
        .create()
    );
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_PRINCIPAL_TYPE)
        .withDescription(resourceString(Constants.RES_PROMPT_PRINCIPAL_TYPE))
        .isRequired()
        .hasArg()
        .create()
    );
    this.addOption(OptionBuilder
        .withLongOpt(Constants.OPT_WITH_GRANT)
        .withDescription(resourceString(Constants.RES_PROMPT_WITH_GRANT))
        .create(Constants.OPT_WITH_GRANT_CHAR)
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return grantPrivilege(
      line.getOptionValue(Constants.OPT_ACTION),
      line.getOptionValue(Constants.OPT_RESOURCE_TYPE),
      line.getOptionValue(Constants.OPT_RESOURCE),
      line.getOptionValue(Constants.OPT_PRINCIPAL_TYPE),
      line.getOptionValue(Constants.OPT_PRINCIPAL),
      line.hasOption(Constants.OPT_WITH_GRANT));
  }

  private Status grantPrivilege(String action, String resourceType, String resource,
                                String principalType, String principal, boolean withGrant)
    throws IOException {
    MResource resourceObject = new MResource(resource, resourceType);
    MPrivilege privilegeObject = new MPrivilege(resourceObject, action, withGrant);
    MPrincipal principalObject = new MPrincipal(principal, principalType);

    client.grantPrivilege(
      Arrays.asList(principalObject),
      Arrays.asList(privilegeObject));

    client.clearCache();

    printlnResource(Constants.RES_GRANT_PRIVILEGE_SUCCESSFUL,
      action, resourceType + " " + resource,
      ((withGrant) ? " " + resourceString(Constants.RES_GRANT_PRIVILEGE_SUCCESSFUL_WITH_GRANT) : ""),
      principalType + " " + principal);

    return Status.OK;
  }
}
