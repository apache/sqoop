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
import org.apache.sqoop.shell.utils.LinkDynamicFormOptions;
import org.apache.sqoop.shell.utils.FormDisplayer;
import org.apache.sqoop.shell.utils.FormOptions;
import org.apache.sqoop.validation.Status;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import static org.apache.sqoop.shell.ShellEnvironment.*;
import static org.apache.sqoop.shell.utils.FormFiller.*;

/**
 *
 */
@SuppressWarnings("serial")
public class UpdateLinkFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public UpdateLinkFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_LINK_ID))
      .withLongOpt(Constants.OPT_LID)
      .isRequired()
      .hasArg()
      .create(Constants.OPT_LID_CHAR));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return updateLink(getLong(line, Constants.OPT_LID), line.getArgList(), isInteractive);
  }

  private Status updateLink(Long linkId, List<String> args, boolean isInteractive) throws IOException {
    printlnResource(Constants.RES_UPDATE_UPDATING_LINK, linkId);

    ConsoleReader reader = new ConsoleReader();

    MLink link = client.getLink(linkId);

    ResourceBundle connectorConfigBundle = client.getConnectorConfigResourceBundle(link.getConnectorId());
    ResourceBundle driverConfigBundle = client.getDriverConfigBundle();

    Status status = Status.FINE;

    if (isInteractive) {
      printlnResource(Constants.RES_PROMPT_UPDATE_LINK_CONFIG);

      do {
        // Print error introduction if needed
        if( !status.canProceed() ) {
          errorIntroduction();
        }

        // Fill in data from user
        if(!fillLink(reader, link, connectorConfigBundle, driverConfigBundle)) {
          return null;
        }

        // Try to create
        status = client.updateLink(link);
      } while(!status.canProceed());
    } else {
      LinkDynamicFormOptions options = new LinkDynamicFormOptions();
      options.prepareOptions(link);
      CommandLine line = FormOptions.parseOptions(options, 0, args, false);
      if (fillConnection(line, link)) {
        status = client.updateLink(link);
        if (!status.canProceed()) {
          printLinkValidationMessages(link);
          return null;
        }
      } else {
        printLinkValidationMessages(link);
        return null;
      }
    }
    FormDisplayer.displayFormWarning(link);
    printlnResource(Constants.RES_UPDATE_LINK_SUCCESSFUL, status.name());

    return status;
  }
}
