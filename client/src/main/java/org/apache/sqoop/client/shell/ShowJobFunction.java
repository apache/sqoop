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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.core.Constants;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.model.MJob;
import org.codehaus.groovy.tools.shell.IO;

import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.List;

import static org.apache.sqoop.client.utils.FormDisplayer.*;
import static org.apache.sqoop.client.core.RequestCache.*;

/**
 *
 */
public class ShowJobFunction extends SqoopFunction {

  private IO io;

  @SuppressWarnings("static-access")
  protected ShowJobFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_ALL_JOBS))
        .withLongOpt(Constants.OPT_ALL)
        .create(Constants.OPT_ALL_CHAR));
    this.addOption(OptionBuilder.hasArg().withArgName(Constants.OPT_JID)
        .withDescription(getResource().getString(Constants
            .RES_SHOW_PROMPT_DISPLAY_JOB_JID))
        .withLongOpt(Constants.OPT_JID)
        .create(Constants.OPT_JID_CHAR));
  }

  public void printHelp(PrintWriter out) {
    out.println(getResource().getString(Constants.RES_SHOW_JOB_USAGE));
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    if (args.size() == 1) {
      printHelp(io.out);
      io.out.println();
      return null;
    }

    CommandLine line = parseOptions(this, 1, args);
    if (line.hasOption(Constants.OPT_ALL)) {
      showJob(null);

    } else if (line.hasOption(Constants.OPT_JID)) {
      showJob(line.getOptionValue(Constants.OPT_JID));
    }

    return null;
  }

  private void showJob(String jid) {
    JobBean jobBean = readJob(jid);

    List<MJob> jobs = jobBean.getJobs();
    String s = MessageFormat.format(Constants
        .RES_SHOW_PROMPT_JOBS_TO_SHOW, jobs.size());
    io.out.println(s);

    for (MJob job : jobs) {
      s = MessageFormat.format(getResource().getString
          (Constants.RES_SHOW_PROMPT_JOB_INFO), job.getPersistenceId(),
          job.getName());
      io.out.println(s);

      long connectorId = job.getConnectorId();

      // Display connector part
      displayForms(io,
                   job.getConnectorPart().getForms(),
                   jobBean.getConnectorBundle(connectorId));
      displayForms(io,
                   job.getFrameworkPart().getForms(),
                   jobBean.getFrameworkBundle());
    }
  }
}
