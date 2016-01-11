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
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.LinkDynamicConfigOptions;
import org.apache.sqoop.shell.utils.ConfigDisplayer;
import org.apache.sqoop.shell.utils.ConfigOptions;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import jline.console.ConsoleReader;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.ConfigFiller.*;

/**
 *
 */
@SuppressWarnings("serial")
public class CreateLinkFunction extends SqoopFunction {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("static-access")
  public CreateLinkFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_CONNECTOR_NAME))
      .withLongOpt(Constants.OPT_CONNECTOR_NAME)
      .isRequired()
      .hasArg()
      .create(Constants.OPT_CONNECTOR_NAME_CHAR));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return createLink(line, line.getArgList(), isInteractive);
  }

  private Status createLink(CommandLine line, List<String> args, boolean isInteractive) throws IOException {
    String connectorName = line.getOptionValue(Constants.OPT_CONNECTOR_NAME);

    //Command line had connector name
    //This will call getConnector() inside createLink()
    MLink link = getClient().createLink(connectorName);
    printlnResource(Constants.RES_CREATE_CREATING_LINK, connectorName);

    ConsoleReader reader = getConsoleReader();

    ResourceBundle connectorConfigBundle = getClient().getConnectorConfigBundle(connectorName);

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
        status = getClient().saveLink(link);
      } while(!status.canProceed());
    } else {
      LinkDynamicConfigOptions options = new LinkDynamicConfigOptions();
      options.prepareOptions(link);
      CommandLine linkoptsline = ConfigOptions.parseOptions(options, 0, args, false);
      if (fillLink(linkoptsline, link)) {
        status = getClient().saveLink(link);
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
    printlnResource(Constants.RES_CREATE_LINK_SUCCESSFUL, status.name(), link.getName());

    return status;
  }
}
