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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.VersionBean;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.MValidator;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.utils.MapResourceBundle;
import org.apache.sqoop.validation.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestShowCommand {
  ShowCommand showCmd;
  SqoopClient client;
  ByteArrayOutputStream out;

//  @BeforeTest(alwaysRun = false)
//  public void setup() {
//    Groovysh shell = new Groovysh();
//    showCmd = new ShowCommand(shell);
//    ShellEnvironment.setInteractive(false);
//    out = new ByteArrayOutputStream();
//    ShellEnvironment.setIo(new IO(System.in, out, System.err));
//    client = mock(SqoopClient.class);
//    ShellEnvironment.setClient(client);
//  }

  @Test(enabled = false)
  public void testShowServer() throws IOException {
    // show server -host -port -webapp
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-host", "-port", "-webapp"));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Server host:"));
    Assert.assertTrue(str.contains("Server port:"));
    Assert.assertTrue(str.contains("Server webapp:"));

    // show server -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-all"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Server host:"));
    Assert.assertTrue(str.contains("Server port:"));
    Assert.assertTrue(str.contains("Server webapp:"));

    // show server -host
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-host"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Server host:"));
    Assert.assertFalse(str.contains("Server port:"));
    Assert.assertFalse(str.contains("Server webapp:"));

    // show server -port
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-port"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertFalse(str.contains("Server host:"));
    Assert.assertTrue(str.contains("Server port:"));
    Assert.assertFalse(str.contains("Server webapp:"));

    // show server -webapp
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-webapp"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertFalse(str.contains("Server host:"));
    Assert.assertFalse(str.contains("Server port:"));
    Assert.assertTrue(str.contains("Server webapp:"));
  }

  @Test(enabled = false)
  public void testShowVersion() {
    when(client.readVersion()).thenReturn(new VersionBean());

    // show version -server -client -api
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_VERSION, "-server", "-client", "-api"));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("server version:"));
    Assert.assertTrue(str.contains("client version:"));
    Assert.assertTrue(str.contains("API versions:"));

    // show version -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_VERSION, "-all"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("server version:"));
    Assert.assertTrue(str.contains("client version:"));
    Assert.assertTrue(str.contains("API versions:"));

    // show client version when no option is specified
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_VERSION));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertFalse(str.contains("server version:"));
    Assert.assertTrue(str.contains("client version:"));
    Assert.assertFalse(str.contains("API versions:"));
  }

  @Test(enabled = false)
  public void testShowConnector() {
    when(client.getConnectors()).thenReturn(new ArrayList<MConnector>());
    when(client.getConnector(any(String.class))).thenReturn(
        new MConnector("test_connector", "", "",
            new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
            new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
            new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>())));

    // show connector summary
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Name"));
    Assert.assertTrue(str.contains("Version"));
    Assert.assertTrue(str.contains("Class"));
    Assert.assertTrue(str.contains("Supported Directions"));

    // show connector -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-all"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("connector(s) to show:"));

    // show connector -name test_connector
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-name", "test_connector"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Connector with Name: test_connector"));
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test(enabled = false)
  public void testShowDriver() {
    when(client.getDriver()).thenReturn(new MDriver(new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()), ""));
    when(client.getDriverConfig()).thenReturn(new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    when(client.getDriverConfigBundle()).thenReturn(new MapResourceBundle(new HashMap()));

    // show driver
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_DRIVER_CONFIG));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Driver specific options:"));
  }

  @Test(enabled = false)
  public void testShowLink() {
    when(client.getLinks()).thenReturn(new ArrayList<MLink>());
    when(client.getLink(any(String.class))).thenReturn(new MLink("connector_test", new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>())));

    // show link summary
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_LINK));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Id"));
    Assert.assertTrue(str.contains("Name"));
    Assert.assertTrue(str.contains("Connector Name"));
    Assert.assertTrue(str.contains("Enabled"));

    // show link -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_LINK, "-all"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("link(s) to show:"));

    // show link -lid 1
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid", "1"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("link with id"));
  }

  @Test(enabled = false)
  public void testShowJob() {
    when(client.getJobs()).thenReturn(new ArrayList<MJob>());
    when(client.getConnector(any(Long.class))).thenReturn(new MConnector("", "", "", null, null, null));
    when(client.getJob("jobName")).thenReturn(new MJob("fromConnectorName", "toConnectorName", "linkName1", "linkName2",
        new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>())));
    when(client.getJobsByConnector("fromConnectorName")).thenReturn(Arrays.asList(new MJob("fromConnectorName", "toConnectorName",
        "linkName1", "linkName2", new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()))));

    // show job summary
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_JOB));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Id"));
    Assert.assertTrue(str.contains("Name"));
    Assert.assertTrue(str.contains("From Connector"));
    Assert.assertTrue(str.contains("To Connector"));
    Assert.assertTrue(str.contains("Enabled"));

    // show job -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_JOB, "-all"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("job(s) to show:"));

    // show job -name jobName
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "jobName"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Job with name"));

    // show job -connector fromConnectorName
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_JOB, "-connector", "fromConnectorName"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("job(s) to show:"));
  }

  @Test(enabled = false)
  public void testShowSubmission() {
    when(client.getSubmissions()).thenReturn(Arrays.asList(new MSubmission(1L)));
    when(client.getSubmissionsForJob(any(String.class))).thenReturn(Arrays.asList(new MSubmission(1L)));

    // show submission -details -name jobName
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION, "-detail", "-name", "jobName"));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Submission details"));

    // show submission -details
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION, "-detail"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Submission details"));

    // show submission -job jobName
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION, "-job", "jobName"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Job Id"));
    Assert.assertTrue(str.contains("External Id"));
    Assert.assertTrue(str.contains("Status"));
    Assert.assertTrue(str.contains("Last Update Date"));

    // show submission
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Job Id"));
    Assert.assertTrue(str.contains("External Id"));
    Assert.assertTrue(str.contains("Status"));
    Assert.assertTrue(str.contains("Last Update Date"));
  }

  @Test(enabled = false)
  public void testShowOption() {
    // show option -name verbose
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "verbose"));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Verbose ="));

    // show option -name poll-timeout
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "poll-timeout"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Poll-timeout ="));

    // show all options
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_OPTION));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Verbose ="));
    Assert.assertTrue(str.contains("Poll-timeout ="));
  }

  @Test(enabled = false)
  public void testShowRole() {
    when(client.getRolesByPrincipal(any(MPrincipal.class))).thenReturn(new ArrayList<MRole>());
    // show role -principal-type user -principal principal_1
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal-type", "user", "-principal", "principal_1"));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Role Name"));

    when(client.getRoles()).thenReturn(new ArrayList<MRole>());
    // show role
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_ROLE));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Role Name"));
  }

  @Test(enabled = false)
  public void testShowPrincipal() {
    when(client.getPrincipalsByRole(any(MRole.class))).thenReturn(new ArrayList<MPrincipal>());
    // show principal -role role_test
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_PRINCIPAL, "-role", "role_test"));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Principal Name"));
    Assert.assertTrue(str.contains("Principal Type"));

    // Missing option role name
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRINCIPAL));
      Assert.fail("Show principal should fail as role name is missing!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @Test(enabled = false)
  public void testShowPrivilege() {
    when(client.getPrincipalsByRole(any(MRole.class))).thenReturn(new ArrayList<MPrincipal>());
    // show privilege -principal-type user -principal principal_test -resource-type connector -resource resource_test
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE,
        "-principal-type", "user", "-principal", "principal_test", "-resource-type", "connector", "-resource", "resource_test"));
    Assert.assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Action"));
    Assert.assertTrue(str.contains("Resource Name"));
    Assert.assertTrue(str.contains("Resource Type"));
    Assert.assertTrue(str.contains("With Grant"));

    // show privilege -principal-type user -principal principal_test
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "user", "-principal", "principal_test"));
    Assert.assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    Assert.assertTrue(str.contains("Action"));
    Assert.assertTrue(str.contains("Resource Name"));
    Assert.assertTrue(str.contains("Resource Type"));
    Assert.assertTrue(str.contains("With Grant"));

    // options resource-type and resource must be used together: missing option resource
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "user", "-principal", "principal_test", "-resource-type", "connector"));
      Assert.fail("Show principal should fail as option resource is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(e.getErrorCode(), ShellError.SHELL_0003);
    }

    // options resource-type and resource must be used together: missing option resource-type
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "user", "-principal", "principal_test", "-resource", "resource_test"));
      Assert.fail("Show principal should fail as option resource-type is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(e.getErrorCode(), ShellError.SHELL_0003);
    }

    // Missing option principal-type
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal", "principal_test", "-resource-type", "connector", "-resource", "resource_test"));
      Assert.fail("Show privilege should fail as option principal-type is missing!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }

    // Missing option principal
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "group", "-resource-type", "connector", "-resource", "resource_test"));
      Assert.fail("Show privilege should fail as option principal is missing!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }
  }
}
