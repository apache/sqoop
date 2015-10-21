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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.MValidator;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestCreateCommand {
  CreateCommand createCmd;
  SqoopClient client;

  @BeforeTest(alwaysRun = true)
  public void setup() {
    Groovysh shell = new Groovysh();
    createCmd = new CreateCommand(shell);
    ShellEnvironment.setInteractive(false);
    ShellEnvironment.setIo(shell.getIo());
    client = mock(SqoopClient.class);
    ShellEnvironment.setClient(client);
  }

  @Test
  public void testCreateLink() {
    when(client.getConnector("connector_1")).thenReturn(new MConnector("", "", "", null, null, null));
    when(client.createLink("connector_1")).thenReturn(new MLink(1, new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>())));
    when(client.saveLink(any(MLink.class))).thenReturn(Status.OK);

    // create link -c connector_1
    Status status = (Status) createCmd.execute(Arrays.asList(Constants.FN_LINK, "-c", "connector_1"));
    Assert.assertTrue(status != null && status == Status.OK);

    // create link -cid connector_1
    status = (Status) createCmd.execute(Arrays.asList(Constants.FN_LINK, "-cid", "connector_1"));
    Assert.assertTrue(status != null && status == Status.OK);

    // incorrect command: create link -c
    try {
      status = (Status) createCmd.execute(Arrays.asList(Constants.FN_LINK, "-c"));
      Assert.fail("Create link should fail as connector id/name is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }
  }

  @Test
  public void testCreateLinkWithNonExistingConnector() {
    when(client.getConnector(any(String.class))).thenThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "Connector doesn't exist"));
    when(client.getConnector(any(Integer.class))).thenThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "Connector doesn't exist"));

    try {
      createCmd.execute(Arrays.asList(Constants.FN_LINK, "-c", "connector_1"));
      Assert.fail("Create link should fail as requested connector doesn't exist!");
    } catch (SqoopException e) {
      Assert.assertEquals(TestShellError.TEST_SHELL_0000, e.getErrorCode());
    }
  }

  @Test
  public void testCreateJob() {
    MConnector fromConnector = new MConnector("connector_from", "", "", null, new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()), null);
    MConnector toConnector = new MConnector("connector_to", "", "", null, null, new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    when(client.createJob("link_from", "link_to")).thenReturn(
        new MJob(1, 2, 1, 2, new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
            new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
            new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>())));
    when(client.getConnector(1)).thenReturn(fromConnector);
    when(client.getConnector(2)).thenReturn(toConnector);
    when(client.saveJob(any(MJob.class))).thenReturn(Status.OK);

    // create job -f link_from -to link_to
    Status status = (Status) createCmd.execute(Arrays.asList(Constants.FN_JOB, "-f", "link_from", "-to", "link_to"));
    Assert.assertTrue(status != null && status == Status.OK);

    // incorrect command: create job -f link_from
    try {
      status = (Status) createCmd.execute(Arrays.asList(Constants.FN_JOB, "-f", "link_from"));
      Assert.fail("Create Job should fail as the to link id/name is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @Test
  public void testCreateJobWithNonExistingLink() {
    when(client.createJob("link_from", "link_to")).thenThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "From link doesn't exist"));

    try {
      createCmd.execute(Arrays.asList(Constants.FN_JOB, "-f", "link_from", "-to", "link_to"));
      Assert.fail("Create Job should fail as from link doesn't exist!");
    } catch (SqoopException e) {
      Assert.assertEquals(TestShellError.TEST_SHELL_0000, e.getErrorCode());
    }
  }

  @Test
  public void testCreateRole() {
    // create role -r role_1
    Status status = (Status) createCmd.execute(Arrays.asList(Constants.FN_ROLE, "-r", "role_1"));
    Assert.assertTrue(status != null && status == Status.OK);
  }
}
