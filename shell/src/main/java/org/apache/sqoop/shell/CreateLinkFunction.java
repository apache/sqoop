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

import jline.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.LinkDynamicConfigOptions;
import org.apache.sqoop.shell.utils.ConfigDisplayer;
import org.apache.sqoop.shell.utils.ConfigOptions;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigFiller.*;

/**
 *
 */
@SuppressWarnings("serial")
public class CreateLinkFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public CreateLinkFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_CONNECTOR_ID))
      .withLongOpt(Constants.OPT_CID)
      .isRequired()
      .hasArg()
      .create(Constants.OPT_CID_CHAR));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return createLink(getLong(line, Constants.OPT_CID), line.getArgList(), isInteractive);
  }

  private Status createLink(long connectorId, List<String> args, boolean isInteractive) throws IOException {
    printlnResource(Constants.RES_CREATE_CREATING_LINK, connectorId);

    ConsoleReader reader = new ConsoleReader();

    MLink link = client.createLink(connectorId);

    ResourceBundle connectorConfigBundle = client.getConnectorConfigBundle(connectorId);

    Status status = Status.OK;
    if (isInteractive) {
      printlnResource(Constants.RES_PROMPT_FILL_LINK_CONFIG);

      do {
        // Print error introduction if needed
        if( !status.canProceed() ) {
          errorIntroduction();
        }

        // Fill in data from user
        if(!fillLinkWithBundle(reader, link, connectorConfigBundle)) {
          return null;
        }

        // Try to create
        status = client.saveLink(link);
      } while(!status.canProceed());
    } else {
      LinkDynamicConfigOptions options = new LinkDynamicConfigOptions();
      options.prepareOptions(link);
      CommandLine line = ConfigOptions.parseOptions(options, 0, args, false);
      if (fillLink(line, link)) {
        status = client.saveLink(link);
        if (!status.canProceed()) {
          printLinkValidationMessages(link);
          return null;
        }
      } else {
        printLinkValidationMessages(link);
        return null;
      }
    }

    ConfigDisplayer.displayConfigWarning(link);
    printlnResource(Constants.RES_CREATE_LINK_SUCCESSFUL, status.name(), link.getPersistenceId());

    return status;
  }
}
