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
import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.client.request.JobRequest;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.model.MJob;
import org.codehaus.groovy.tools.shell.IO;

import java.io.PrintWriter;
import java.util.List;

import static org.apache.sqoop.client.utils.FormDisplayer.*;

/**
 *
 */
public class ShowJobFunction extends SqoopFunction {

  public static final String ALL = "all";
  public static final String JID = "jid";

  private IO io;
  private JobRequest jobRequest;

  @SuppressWarnings("static-access")
  protected ShowJobFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
        .withDescription("Display all jobs")
        .withLongOpt(ALL)
        .create(ALL.charAt(0)));
    this.addOption(OptionBuilder.hasArg().withArgName("jid")
        .withDescription("Display job with given jid" )
        .withLongOpt(JID)
        .create('j'));
  }

  public void printHelp(PrintWriter out) {
    out.println("Usage: show job");
    super.printHelp(out);
  }

  public Object execute(List<String> args) {
    if (args.size() == 1) {
      printHelp(io.out);
      io.out.println();
      return null;
    }

    CommandLine line = parseOptions(this, 1, args);
    if (line.hasOption(ALL)) {
      showJob(null);

    } else if (line.hasOption(JID)) {
      showJob(line.getOptionValue(JID));
    }

    return null;
  }

  private void showJob(String jid) {
    if (jobRequest == null) {
      jobRequest = new JobRequest();
    }
    JobBean jobBean = jobRequest.read(Environment.getServerUrl(), jid);

    List<MJob> jobs = jobBean.getJobs();

    io.out.println("@|bold " + jobs.size()
      + " job(s) to show: |@");

    for (MJob job : jobs) {
      io.out.println("Job with id " + job.getPersistenceId()
        + " and name: " + job.getName());

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
