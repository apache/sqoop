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

import java.util.Arrays;
import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.validation.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGrantCommand {
  GrantCommand grantCmd;
  SqoopClient client;

//  @BeforeTest(alwaysRun = false)
//  public void setup() {
//    Groovysh shell = new Groovysh();
//    grantCmd = new GrantCommand(shell);
//    ShellEnvironment.setInteractive(false);
//    ShellEnvironment.setIo(shell.getIo());
//    client = mock(SqoopClient.class);
//    ShellEnvironment.setClient(client);
//  }

  @SuppressWarnings("unchecked")
  @Test(enabled = false)
  public void testGrantRole() {
    doNothing().when(client).grantRole(any(List.class), any(List.class));

    // grant role -principal_type user -principal principal_test -role role_test
    Status status = (Status) grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal-type", "user", "-principal", "principal_test", "-role", "role_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // principal_type is not correct
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal-type", "non_existing_principal_type", "-principal", "principal_test", "-role", "role_test"));
      Assert.fail("Grant role should fail as principal-type is not among user/group/role!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("No enum constant"));
    }

    // Missing argument for principal_type
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal-type"));
      Assert.fail("Grant role should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing argument for principal
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal"));
      Assert.fail("Grant role should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing argument for role name
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-role"));
      Assert.fail("Grant role should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing options principal-type and principal
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-role", "role_test"));
      Assert.fail("Grant role should fail as of missing required options!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }

    // Missing options principal-type and role name
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal", "principal_test"));
      Assert.fail("Grant role should fail as of missing required options!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }

    // Missing options principal and role name
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal-type", "role"));
      Assert.fail("Grant role should fail as of missing required options!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }

    // Missing option principal-type
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-role", "role_test", "-principal", "principal_test"));
      Assert.fail("Grant role should fail as of missing required options!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }

    // Missing option role
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_ROLE, "-principal-type", "group", "-principal", "principal_test"));
      Assert.fail("Grant role should fail as of missing required options!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @SuppressWarnings("unchecked")
  @Test(enabled = false)
  public void testGrantPrivilege() {
    doNothing().when(client).grantPrivilege(any(List.class), any(List.class));

    // grant privilege -resource-type connector -resource resource_test -action read -principal principal_test -principal_type group -with-grant
    Status status = (Status) grantCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-resource-type", "connector", "-resource", "resource_test", "-action", "read", "-principal", "principal_test", "-principal-type", "group", "-with-grant"));
    Assert.assertTrue(status != null && status == Status.OK);

    // resource-type is not correct
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-resource-type", "non_existing_resource_type", "-resource", "resource_test", "-action", "read", "-principal", "principal_test", "-principal-type", "group", "-with-grant"));
      Assert.fail("Grant privilege should fail as resource-type is not among server/connector/link/job!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("No enum constant"));
    }

    // action is not correct
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-resource-type", "connector", "-resource", "resource_test", "-action", "non_existing_action", "-principal", "principal_test", "-principal-type", "group", "-with-grant"));
      Assert.fail("Grant privilege should fail as action is not among read/write/all!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("No enum constant"));
    }

    // principal-type is not correct
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-resource-type", "connector", "-resource", "resource_test", "-action", "write", "-principal", "principal_test", "-principal-type", "non_existing_principal_type", "-with-grant"));
      Assert.fail("Grant privilege should fail as principal-type is not among user/group/role!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("No enum constant"));
    }

    // Missing argument for option resource-type
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-resource-type", "-resource", "resource_test", "-action", "write", "-principal", "principal_test", "-principal-type", "non_existing_principal_type", "-with-grant"));
      Assert.fail("Grant privilege should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing option principal-type
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-resource-type", "connector", "-resource", "resource_test", "-action", "write", "-principal", "principal_test", "-with-grant"));
      Assert.fail("Grant privilege should fail as of missing required options!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }

    // Missing option action
    try {
      grantCmd.execute(Arrays.asList(Constants.FN_PRIVILEGE, "-resource-type", "connector", "-resource", "resource_test", "-principal", "principal_test", "-principal-type", "group", "-with-grant"));
      Assert.fail("Grant privilege should fail as of missing required options!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }
  }
}
