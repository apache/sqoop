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
package org.apache.sqoop.integration.shell;

import org.apache.sqoop.shell.ShellEnvironment;
import org.apache.sqoop.shell.ShowCommand;
import org.apache.sqoop.shell.SqoopCommand;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProviderForShellTest;
import org.apache.sqoop.test.testcases.ShellTestCase;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static org.testng.Assert.*;

@Infrastructure(dependencies = {SqoopInfrastructureProviderForShellTest.class, DatabaseInfrastructureProvider.class})
public class ShowCommandTest extends ShellTestCase {
  private ByteArrayOutputStream out;

  protected SqoopCommand createCommand(Groovysh shell) {
    return new ShowCommand(shell);
  }

  @Test
  public void testShowOption() {
    resetIO();

    // show option -name verbose
    Status status = (Status) execute(Arrays.asList(Constants.FN_OPTION, "-name", "verbose"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Verbose ="));

    // show option -name poll-timeout
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_OPTION, "-name", "poll-timeout"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Poll-timeout ="));

    // show all options
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_OPTION));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Verbose ="));
    assertTrue(str.contains("Poll-timeout ="));
  }

  @Test
  public void testShowServer() throws Exception {
    resetIO();

    // show server -host -port -webapp
    Status status = (Status)  execute(Arrays.asList(Constants.FN_SERVER, "-host", "-port", "-webapp"));
    assertTrue(status != null && status == Status.OK);
    String outPuts = new String(out.toByteArray());
    assertTrue(outPuts.contains("Server host:"));
    assertTrue(outPuts.contains("Server port:"));
    assertTrue(outPuts.contains("Server webapp:"));

    // show server -all
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_SERVER, "-all"));
    assertTrue(status != null && status == Status.OK);
    outPuts = new String(out.toByteArray());
    assertTrue(outPuts.contains("Server host:"));
    assertTrue(outPuts.contains("Server port:"));
    assertTrue(outPuts.contains("Server webapp:"));

    // show server -host
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_SERVER, "-host"));
    assertTrue(status != null && status == Status.OK);
    outPuts = new String(out.toByteArray());
    assertTrue(outPuts.contains("Server host:"));
    assertFalse(outPuts.contains("Server port:"));
    assertFalse(outPuts.contains("Server webapp:"));

    // show server -port
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_SERVER, "-port"));
    assertTrue(status != null && status == Status.OK);
    outPuts = new String(out.toByteArray());
    assertFalse(outPuts.contains("Server host:"));
    assertTrue(outPuts.contains("Server port:"));
    assertFalse(outPuts.contains("Server webapp:"));

    // show server -webapp
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_SERVER, "-webapp"));
    assertTrue(status != null && status == Status.OK);
    outPuts = new String(out.toByteArray());
    assertFalse(outPuts.contains("Server host:"));
    assertFalse(outPuts.contains("Server port:"));
    assertTrue(outPuts.contains("Server webapp:"));
  }

  @Test
  public void testShowVersion() {
    resetIO();

    // show version -server -client -api
    Status status = (Status) execute(Arrays.asList(Constants.FN_VERSION, "-server", "-client", "-api"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("server version:"));
    assertTrue(str.contains("client version:"));
    assertTrue(str.contains("API versions:"));

    // show version -all
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_VERSION, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("server version:"));
    assertTrue(str.contains("client version:"));
    assertTrue(str.contains("API versions:"));

    // show client version when no option is specified
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_VERSION));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertFalse(str.contains("server version:"));
    assertTrue(str.contains("client version:"));
    assertFalse(str.contains("API versions:"));
  }

  @Test
  public void testShowConnector() {
    resetIO();

    // show connector summary
    Status status = (Status) execute(Arrays.asList(Constants.FN_CONNECTOR));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Name"));
    assertTrue(str.contains("Version"));
    assertTrue(str.contains("Class"));
    assertTrue(str.contains("Supported Directions"));

    // show connector -all
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_CONNECTOR, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("connector(s) to show:"));

    // show connector -name test_connector
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_CONNECTOR, "-name", "generic-jdbc-connector"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Connector with Name: generic-jdbc-connector"));
  }

  @Test
  public void testShowDriver() {
    resetIO();

    // show driver
    Status status = (Status) execute(Arrays.asList(Constants.FN_DRIVER_CONFIG));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Driver specific options:"));
  }

  @Test
  public void testShowLink() {
    createLink("linkName");
    resetIO();

    // show link summary
    Status status = (Status) execute(Arrays.asList(Constants.FN_LINK));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Name"));
    assertTrue(str.contains("Connector Name"));
    assertTrue(str.contains("Enabled"));

    // show link -all
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_LINK, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("link(s) to show:"));

    // show link -name linkName
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_LINK, "-name", "linkName"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("link with name"));
  }

  @Test
  public void testShowJob() {
    createJob("fromLink", "toLink", "jobName");
    resetIO();

    // show job summary
    Status status = (Status) execute(Arrays.asList(Constants.FN_JOB));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Id"));
    assertTrue(str.contains("Name"));
    assertTrue(str.contains("From Connector"));
    assertTrue(str.contains("To Connector"));
    assertTrue(str.contains("Enabled"));

    // show job -all
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("job(s) to show:"));

    // show job -name jobName
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-name", "jobName"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Job with name"));

    // show job -connector generic-jdbc-connector
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-connector", "generic-jdbc-connector"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("job(s) to show:"));
  }

  @Test
  public void testShowSubmission() throws Exception {
    createJob("fromLink", "toLink", "jobForShowCommand");
    executeJob("jobForShowCommand", false);
    resetIO();

    // show submission -details -job jobName
    Status status = (Status) execute(Arrays.asList(Constants.FN_SUBMISSION, "-detail", "-job", "jobForShowCommand"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Submission details"));

    // show submission -details
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_SUBMISSION, "-detail"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Submission details"));

    // show submission -job jobName
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_SUBMISSION, "-job", "jobForShowCommand"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Job Name"));
    assertTrue(str.contains("External Id"));
    assertTrue(str.contains("Status"));
    assertTrue(str.contains("Last Update Date"));

    // show submission
    out.reset();
    status = (Status) execute(Arrays.asList(Constants.FN_SUBMISSION));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Job Name"));
    assertTrue(str.contains("External Id"));
    assertTrue(str.contains("Status"));
    assertTrue(str.contains("Last Update Date"));
  }

  private void resetIO() {
    out = new ByteArrayOutputStream();
    ShellEnvironment.setIo(new IO(System.in, out, System.err));
  }
}
