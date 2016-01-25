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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import java.util.Arrays;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.validation.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDeleteCommand {
  DeleteCommand deleteCmd;
  SqoopClient client;

//  @BeforeTest(alwaysRun = false)
//  public void setup() {
//    Groovysh shell = new Groovysh();
//    deleteCmd = new DeleteCommand(shell);
//    ShellEnvironment.setInteractive(false);
//    ShellEnvironment.setIo(shell.getIo());
//    client = mock(SqoopClient.class);
//    ShellEnvironment.setClient(client);
//  }

  @Test(enabled = false)
  public void testDeleteLink() {
    doNothing().when(client).deleteLink("link_test");

    // delete link -l link_test
    Status status = (Status) deleteCmd.execute(Arrays.asList(Constants.FN_LINK, "-l", "link_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // Missing argument for option lid
    try {
      status = (Status) deleteCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid"));
      Assert.fail("Delete link should fail as link id/name is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }
  }

  @Test(enabled = false)
  public void testDeleteLinkWithNonExistingLink() {
    doThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "link doesn't exist")).when(client).deleteLink(any(String.class));

    try {
      deleteCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid", "link_test"));
      Assert.fail("Delete link should fail as requested link doesn't exist!");
    } catch (SqoopException e) {
      Assert.assertEquals(TestShellError.TEST_SHELL_0000, e.getErrorCode());
    }
  }

  @Test(enabled = false)
  public void testDeleteJob() {
    doNothing().when(client).deleteJob("job_test");

    // delete job -name job_test
    Status status = (Status) deleteCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "job_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // Missing argument for option name
    try {
      status = (Status) deleteCmd.execute(Arrays.asList(Constants.FN_JOB, "-name"));
      Assert.fail("Delete job should fail as job name is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }
  }

  @Test(enabled = false)
  public void testDeleteJobWithNonExistingJob() {
    doThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "job doesn't exist")).when(client).deleteJob(any(String.class));

    try {
      deleteCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "job_test"));
      Assert.fail("Delete job should fail as requested job doesn't exist!");
    } catch (SqoopException e) {
      Assert.assertEquals(TestShellError.TEST_SHELL_0000, e.getErrorCode());
    }
  }

  @Test(enabled = false)
  public void testDeleteRole() {
    doNothing().when(client).dropRole(any(MRole.class));

    // delete role -r role_test
    Status status = (Status) deleteCmd.execute(Arrays.asList(Constants.FN_ROLE, "-r", "role_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // Missing argument for option role
    try {
      status = (Status) deleteCmd.execute(Arrays.asList(Constants.FN_ROLE, "-role"));
      Assert.fail("Delete role should fail as role name is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }
  }

  @Test(enabled = false)
  public void testDeleteRoleWithNonExistingRole() {
    doThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "role doesn't exist")).when(client).dropRole(any(MRole.class));

    try {
      deleteCmd.execute(Arrays.asList(Constants.FN_ROLE, "-role", "role_test"));
      Assert.fail("Delete role should fail as requested role doesn't exist!");
    } catch (SqoopException e) {
      Assert.assertEquals(TestShellError.TEST_SHELL_0000, e.getErrorCode());
    }
  }
}
