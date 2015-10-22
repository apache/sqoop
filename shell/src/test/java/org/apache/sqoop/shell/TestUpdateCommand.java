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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.MValidator;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.utils.MapResourceBundle;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestUpdateCommand {
  UpdateCommand updateCmd;
  SqoopClient client;

  @BeforeTest(alwaysRun = true)
  public void setup() {
    Groovysh shell = new Groovysh();
    updateCmd = new UpdateCommand(shell);
    ShellEnvironment.setInteractive(false);
    ShellEnvironment.setIo(shell.getIo());
    client = mock(SqoopClient.class);
    ShellEnvironment.setClient(client);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testUpdateLink() throws InterruptedException {
    MLink link = new MLink(1L, new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    when(client.getLink("link_test")).thenReturn(link);
    when(client.getConnectorConfigBundle(1L)).thenReturn(new MapResourceBundle(new HashMap()));
    when(client.updateLink(link)).thenReturn(Status.OK);

    // update link -lid link_test
    Status status = (Status) updateCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid", "link_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // Missing argument for option lid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid"));
      Assert.fail("Update link should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing option lid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_LINK));
      Assert.fail("Update link should fail as option lid is missing");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testUpdateJob() throws InterruptedException {
    MJob job = new MJob(1L, 2L, 1L, 2L,
        new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    when(client.getJob("job_test")).thenReturn(job);
    when(client.getConnectorConfigBundle(any(Long.class))).thenReturn(new MapResourceBundle(new HashMap()));
    when(client.getDriverConfigBundle()).thenReturn(new MapResourceBundle(new HashMap()));
    when(client.updateJob(job)).thenReturn(Status.OK);

    // update job -jid job_test
    Status status = (Status) updateCmd.execute(Arrays.asList(Constants.FN_JOB, "-jid", "job_test"));
    Assert.assertTrue(status != null && status == Status.OK);

    // Missing argument for option jid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_JOB, "-jid"));
      Assert.fail("Update job should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing option jid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_JOB));
      Assert.fail("Update job should fail as option jid is missing");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing required option"));
    }
  }
}
