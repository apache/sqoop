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

import org.apache.sqoop.client.ClientError;
import org.apache.sqoop.common.SqoopException;

import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.shell.CloneCommand;
import org.apache.sqoop.shell.ShellEnvironment;
import org.apache.sqoop.shell.SqoopCommand;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProviderForShellTest;
import org.apache.sqoop.test.testcases.ShellTestCase;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Infrastructure(dependencies = {SqoopInfrastructureProviderForShellTest.class, DatabaseInfrastructureProvider.class})
public class CloneCommandTest extends ShellTestCase {

  protected SqoopCommand createCommand(Groovysh shell) {
    return new CloneCommand(shell);
  }

  @Test
  public void testCloneLink() throws UnsupportedEncodingException {
    ShellEnvironment.setInteractive(true);
    initEnv();

    // create link
    createLink("linkName");

    // do the clone test
    initData("Clone\n" +                 // link name: append to the old link name
            "linkConfig1\n" +            // link config1
            "linkConfig2\n");            // link config2
    Status status = (Status) execute(Arrays.asList(Constants.FN_LINK, "-name", "linkName"));
    assertTrue(status != null && status != Status.ERROR);
    // the origin link is still there
    MLink orginLink = getClient().getLink("linkName");
    assertEquals(orginLink.getName(), "linkName");

    MLink link = getClient().getLink("linkNameClone");
    assertEquals(link.getName(), "linkNameClone");
    assertEquals(link.getConnectorLinkConfig("testLinkConfigForShell").getInput("testLinkConfigForShell.linkConfig1").getValue(),
            "linkConfig1");
    assertEquals(link.getConnectorLinkConfig("testLinkConfigForShell").getInput("testLinkConfigForShell.linkConfig2").getValue(),
            "linkConfig2");
  }

  @Test
  public void testCloneNonExistingLink() {
    ShellEnvironment.setInteractive(false);
    try {
      execute(Arrays.asList(Constants.FN_LINK, "-name", "non-exist-link"));
      fail("Clone Link should fail as link doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }

  @Test
  public void testCloneJob() throws UnsupportedEncodingException {
    ShellEnvironment.setInteractive(true);
    initEnv();

    createJob("fromLink", "toLink", "jobName");

    // create job -f link_from -to link_to
    initData("Clone\n" +                              // job name
            "fromJobConfig1\n" +                      // from job config1
            "fromJobConfig2\n" +                      // from job config2
            "toJobConfig1\n" +                        // to job config1
            "toJobConfig2\n\n\n\n\n\n");              // to job config2 and nothing for driver
    Status status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-name", "jobName"));
    assertTrue(status != null && status != Status.ERROR);
    // the origin job is still there
    MJob originJob = getClient().getJob("jobName");
    assertEquals(originJob.getName(), "jobName");

    MJob job = getClient().getJob("jobNameClone");
    assertEquals(job.getName(), "jobNameClone");
    assertEquals(job.getFromJobConfig().getInput("testFromJobConfigForShell.fromJobConfig1").getValue(), "fromJobConfig1");
    assertEquals(job.getFromJobConfig().getInput("testFromJobConfigForShell.fromJobConfig2").getValue(), "fromJobConfig2");
    assertEquals(job.getToJobConfig().getInput("testToJobConfigForShell.toJobConfig1").getValue(), "toJobConfig1");
    assertEquals(job.getToJobConfig().getInput("testToJobConfigForShell.toJobConfig2").getValue(), "toJobConfig2");
  }

  @Test
  public void testCloneNonExistingJob() {
    ShellEnvironment.setInteractive(false);
    try {
      execute(Arrays.asList(Constants.FN_JOB, "-name", "non-exist-jobName"));
      fail("Clone Job should fail as job doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }
}
