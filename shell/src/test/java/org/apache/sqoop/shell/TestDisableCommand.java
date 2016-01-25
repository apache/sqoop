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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import java.util.Arrays;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.validation.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableCommand {
  DisableCommand disableCmd;
  SqoopClient client;

//  @BeforeTest(alwaysRun = false)
//  public void setup() {
//    Groovysh shell = new Groovysh();
//    disableCmd = new DisableCommand(shell);
//    ShellEnvironment.setInteractive(false);
//    ShellEnvironment.setIo(shell.getIo());
//    client = mock(SqoopClient.class);
//    ShellEnvironment.setClient(client);
//  }

  @Test(enabled = false)
  public void testDisableLink() {
    doNothing().when(client).enableLink("link_test", false);

    // disable link -l link_test
    Status status = (Status) disableCmd.execute(Arrays.asList(Constants.FN_LINK, "-l", "link_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // Missing argument for option lid
    try {
      status = (Status) disableCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid"));
      Assert.fail("Disable link should fail as link id/name is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }
  }

  @Test(enabled = false)
  public void testDisableLinkWithNonExistingLink() {
    doThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "link doesn't exist")).when(client).enableLink(any(String.class), any(Boolean.class));

    try {
      disableCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid", "link_test"));
      Assert.fail("Disable link should fail as requested link doesn't exist!");
    } catch (SqoopException e) {
      Assert.assertEquals(TestShellError.TEST_SHELL_0000, e.getErrorCode());
    }
  }

  @Test(enabled = false)
  public void testDisableJob() {
    doNothing().when(client).enableJob("job_test", false);

    // disable job -j job_test
    Status status = (Status) disableCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "job_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // Missing argument for option name
    try {
      status = (Status) disableCmd.execute(Arrays.asList(Constants.FN_JOB, "-name"));
      Assert.fail("Disable job should fail as job name is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }
  }

  @Test(enabled = false)
  public void testDisableJobWithNonExistingJob() {
    doThrow(new SqoopException(TestShellError.TEST_SHELL_0000, "job doesn't exist")).when(client).enableJob(any(String.class), any(Boolean.class));

    try {
      disableCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "job_test"));
      Assert.fail("Disable job should fail as requested job doesn't exist!");
    } catch (SqoopException e) {
      Assert.assertEquals(TestShellError.TEST_SHELL_0000, e.getErrorCode());
    }
  }
}
