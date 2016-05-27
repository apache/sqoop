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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.Direction;
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
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestShowCommand {
  ShowCommand showCmd;
  SqoopClient client;
  ByteArrayOutputStream out;

  @BeforeTest(alwaysRun = true)
  public void setup() {
    Groovysh shell = new Groovysh();
    showCmd = new ShowCommand(shell);
    ShellEnvironment.setInteractive(false);
    out = new ByteArrayOutputStream();
    ShellEnvironment.setIo(new IO(System.in, out, System.err));
    client = mock(SqoopClient.class);
    ShellEnvironment.setClient(client);
  }

  @Test
  public void testShowServer() throws IOException {
    // show server -host -port -webapp
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-host", "-port", "-webapp"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Server host:"));
    assertTrue(str.contains("Server port:"));
    assertTrue(str.contains("Server webapp:"));

    // show server -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Server host:"));
    assertTrue(str.contains("Server port:"));
    assertTrue(str.contains("Server webapp:"));

    // show server -host
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-host"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Server host:"));
    assertFalse(str.contains("Server port:"));
    assertFalse(str.contains("Server webapp:"));

    // show server -port
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-port"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertFalse(str.contains("Server host:"));
    assertTrue(str.contains("Server port:"));
    assertFalse(str.contains("Server webapp:"));

    // show server -webapp
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SERVER, "-webapp"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertFalse(str.contains("Server host:"));
    assertFalse(str.contains("Server port:"));
    assertTrue(str.contains("Server webapp:"));
  }

  @Test
  public void testShowVersion() {
    when(client.readVersion()).thenReturn(new VersionBean());

    // show version -server -client -api
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_VERSION, "-server", "-client", "-api"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("server version:"));
    assertTrue(str.contains("client version:"));
    assertTrue(str.contains("API versions:"));

    // show version -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_VERSION, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("server version:"));
    assertTrue(str.contains("client version:"));
    assertTrue(str.contains("API versions:"));

    // show client version when no option is specified
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_VERSION));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertFalse(str.contains("server version:"));
    assertTrue(str.contains("client version:"));
    assertFalse(str.contains("API versions:"));
  }

  @Test
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
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Name"));
    assertTrue(str.contains("Version"));
    assertTrue(str.contains("Class"));
    assertTrue(str.contains("Supported Directions"));

    // show connector -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("connector(s) to show:"));

    // show connector -name test_connector
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-name", "test_connector"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Connector with Name: test_connector"));
  }

  @Test
  public void testShowConnectorsByDirection() {
    Collection<MConnector> connectorsFrom = new ArrayList<MConnector>();
    connectorsFrom.add(new MConnector("from_connector", "", "",
        new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        null));
    when(client.getConnectorsByDirection(Direction.FROM)).thenReturn(connectorsFrom);

    Collection<MConnector> connectorsTo = new ArrayList<MConnector>();
    connectorsTo.add(new MConnector("to_connector", "", "",
        new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        null,
        new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>())));
    when(client.getConnectorsByDirection(Direction.TO)).thenReturn(connectorsTo);

    // show connector -direction from
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-direction", "from"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Connector with Name: from_connector"));

    // show connector -direction to
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-direction", "to"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Connector with Name: to_connector"));

    // show connector -direction
    try {
      out.reset();
      showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-direction"));
      fail("Show connector should fail as option direction is missing!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ShellError.SHELL_0003);
    }

    // show connector -direction misc
    try {
      out.reset();
      showCmd.execute(Arrays.asList(Constants.FN_CONNECTOR, "-direction", "misc"));
      fail("Show connector should fail as option direction is invalid!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ShellError.SHELL_0003);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testShowDriver() {
    when(client.getDriver()).thenReturn(new MDriver(new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()), ""));
    when(client.getDriverConfig()).thenReturn(new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    when(client.getDriverConfigBundle()).thenReturn(new MapResourceBundle(new HashMap()));

    // show driver
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_DRIVER_CONFIG));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Driver specific options:"));
  }

  @Test
  public void testShowLink() {
    MLink fakeLink = new MLink("connector_test", new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    fakeLink.setName("linkName");
    when(client.getLinks()).thenReturn(new ArrayList<MLink>());
    when(client.getLink(any(String.class))).thenReturn(fakeLink);

    // show link summary
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_LINK));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Name"));
    assertTrue(str.contains("Connector Name"));
    assertTrue(str.contains("Enabled"));

    // show link -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_LINK, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("link(s) to show:"));

    // show link -name linkName
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_LINK, "-name", "linkName"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("link with name"));
  }

  @Test
  public void testShowJob() {
    when(client.getJobs()).thenReturn(new ArrayList<MJob>());
    when(client.getConnector(any(String.class))).thenReturn(new MConnector("", "", "", null, null, null));
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
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Id"));
    assertTrue(str.contains("Name"));
    assertTrue(str.contains("From Connector"));
    assertTrue(str.contains("To Connector"));
    assertTrue(str.contains("Enabled"));

    // show job -all
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_JOB, "-all"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("job(s) to show:"));

    // show job -name jobName
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "jobName"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Job with name"));

    // show job -connector fromConnectorName
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_JOB, "-connector", "fromConnectorName"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("job(s) to show:"));
  }

  @Test
  public void testShowSubmission() {
    when(client.getSubmissions()).thenReturn(Arrays.asList(new MSubmission("jobName")));
    when(client.getSubmissionsForJob(any(String.class))).thenReturn(Arrays.asList(new MSubmission("jobName")));

    // show submission -details -job jobName
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION, "-detail", "-job", "jobName"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Submission details"));

    // show submission -details
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION, "-detail"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Submission details"));

    // show submission -job jobName
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION, "-job", "jobName"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Job Name"));
    assertTrue(str.contains("External Id"));
    assertTrue(str.contains("Status"));
    assertTrue(str.contains("Last Update Date"));

    // show submission
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_SUBMISSION));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Job Name"));
    assertTrue(str.contains("External Id"));
    assertTrue(str.contains("Status"));
    assertTrue(str.contains("Last Update Date"));
  }

  @Test
  public void testShowOption() {
    // show option -name verbose
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "verbose"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Verbose ="));

    // show option -name poll-timeout
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "poll-timeout"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Poll-timeout ="));

    // show all options
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_OPTION));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Verbose ="));
    assertTrue(str.contains("Poll-timeout ="));
  }

  @Test
  public void testShowRole() {
    when(client.getRolesByPrincipal(any(MPrincipal.class))).thenReturn(new ArrayList<MRole>());
    // show role -principal-type user -principal principal_1
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal-type", "user", "-principal", "principal_1"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Role Name"));

    when(client.getRoles()).thenReturn(new ArrayList<MRole>());
    // show role
    out.reset();
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_ROLE));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Role Name"));
  }

  @Test
  public void testShowPrincipal() {
    when(client.getPrincipalsByRole(any(MRole.class))).thenReturn(new ArrayList<MPrincipal>());
    // show principal -role role_test
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_PRINCIPAL, "-role", "role_test"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Principal Name"));
    assertTrue(str.contains("Principal Type"));

    // Missing option role name
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRINCIPAL));
      fail("Show principal should fail as role name is missing!");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @Test
  public void testShowPrivilege() {
    when(client.getPrincipalsByRole(any(MRole.class))).thenReturn(new ArrayList<MPrincipal>());
    // show privilege -principal-type user -principal principal_test -resource-type connector -resource resource_test
    out.reset();
    Status status = (Status) showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE,
        "-principal-type", "user", "-principal", "principal_test", "-resource-type", "connector", "-resource", "resource_test"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Action"));
    assertTrue(str.contains("Resource Name"));
    assertTrue(str.contains("Resource Type"));
    assertTrue(str.contains("With Grant"));

    // show privilege -principal-type user -principal principal_test
    status = (Status) showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "user", "-principal", "principal_test"));
    assertTrue(status != null && status == Status.OK);
    str = new String(out.toByteArray());
    assertTrue(str.contains("Action"));
    assertTrue(str.contains("Resource Name"));
    assertTrue(str.contains("Resource Type"));
    assertTrue(str.contains("With Grant"));

    // options resource-type and resource must be used together: missing option resource
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "user", "-principal", "principal_test", "-resource-type", "connector"));
      fail("Show principal should fail as option resource is missing!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ShellError.SHELL_0003);
    }

    // options resource-type and resource must be used together: missing option resource-type
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "user", "-principal", "principal_test", "-resource", "resource_test"));
      fail("Show principal should fail as option resource-type is missing!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ShellError.SHELL_0003);
    }

    // Missing option principal-type
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal", "principal_test", "-resource-type", "connector", "-resource", "resource_test"));
      fail("Show privilege should fail as option principal-type is missing!");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Missing required option"));
    }

    // Missing option principal
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-principal-type", "group", "-resource-type", "connector", "-resource", "resource_test"));
      fail("Show privilege should fail as option principal is missing!");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @Test
  public void testUnknowOption() {
    try {
      showCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-unknownOption"));
      fail("Show principal should fail as unknown option encountered!");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Unknown option encountered"));
    }
  }
}
