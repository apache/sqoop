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
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.LinkDynamicConfigOptions;
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
public class CloneLinkFunction extends SqoopFunction {
  @SuppressWarnings("static-access")
  public CloneLinkFunction() {
    this.addOption(OptionBuilder
      .withDescription(resourceString(Constants.RES_PROMPT_LINK_ID))
      .withLongOpt(Constants.OPT_LID)
      .hasArg()
      .isRequired()
      .create(Constants.OPT_LID_CHAR)
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeFunction(CommandLine line, boolean isInteractive) throws IOException {
    return cloneLink(getLong(line, Constants.OPT_LID), line.getArgList(), isInteractive);
  }

  private Status cloneLink(Long connectionId, List<String> args, boolean isInteractive) throws IOException {
    printlnResource(Constants.RES_CLONE_CLONING_LINK, connectionId);

    ConsoleReader reader = new ConsoleReader();

    MLink connection = client.getLink(connectionId);
    // Remove persistent id as we're making a clone
    connection.setPersistenceId(MPersistableEntity.PERSISTANCE_ID_DEFAULT);

    Status status = Status.OK;

    ResourceBundle linkConfigBundle = client.getConnectorConfigBundle(connection.getConnectorId());

    if (isInteractive) {
      printlnResource(Constants.RES_PROMPT_UPDATE_LINK_CONFIG);

      do {
        // Print error introduction if needed
        if( !status.canProceed() ) {
          errorIntroduction();
        }

        // Fill in data from user
        if(!fillLinkWithBundle(reader, connection, linkConfigBundle)) {
          return null;
        }

        status = client.saveLink(connection);
      } while(!status.canProceed());
    } else {
      LinkDynamicConfigOptions options = new LinkDynamicConfigOptions();
      options.prepareOptions(connection);
      CommandLine line = ConfigOptions.parseOptions(options, 0, args, false);
      if (fillLink(line, connection)) {
        status = client.saveLink(connection);
        if (!status.canProceed()) {
          printLinkValidationMessages(connection);
          return null;
        }
      } else {
        printLinkValidationMessages(connection);
        return null;
      }
    }

    printlnResource(Constants.RES_CLONE_LINK_SUCCESSFUL, status.name(), connection.getPersistenceId());

    return status;
  }
}
