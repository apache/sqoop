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
import org.apache.sqoop.client.core.RequestCache;
import org.apache.sqoop.client.utils.SubmissionDisplayer;
import org.apache.sqoop.model.MSubmission;
import org.codehaus.groovy.tools.shell.IO;

import java.util.List;

/**
 *
 */
public class SubmissionStartFunction extends SqoopFunction {
  private static final String JID = "jid";

  private IO io;

  @SuppressWarnings("static-access")
  public SubmissionStartFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder
      .withDescription("Job ID")
      .withLongOpt(JID)
      .hasArg()
      .create(JID.charAt(0)));
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (!line.hasOption(JID)) {
      io.out.println("Required argument --jid is missing.");
      return null;
    }

    MSubmission submission =
      RequestCache.createSubmission(line.getOptionValue(JID));

    SubmissionDisplayer.display(io, submission);
    return null;
  }
}
