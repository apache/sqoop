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
import org.apache.sqoop.shell.DisableCommand;
import org.apache.sqoop.shell.SqoopCommand;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProviderForShellTest;
import org.apache.sqoop.test.testcases.ShellTestCase;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.*;

@Infrastructure(dependencies = {SqoopInfrastructureProviderForShellTest.class, DatabaseInfrastructureProvider.class})
public class DisableCommandTest extends ShellTestCase {

  protected SqoopCommand createCommand(Groovysh shell) {
    return new DisableCommand(shell);
  }

  @Test
  public void testDisableLink() {
    // creaet link
    createLink("linkName");

    // the link is enable
    MLink link = getClient().getLink("linkName");
    assertTrue(link.getEnabled());

    // disable link -name linkName
    Status status = (Status) execute(Arrays.asList(Constants.FN_LINK, "-name", "linkName"));
    assertTrue(status != null && status == Status.OK);

    // the link is disable
    link = getClient().getLink("linkName");
    assertFalse(link.getEnabled());

    // disable the non-exist link
    try {
      execute(Arrays.asList(Constants.FN_LINK, "-name", "non-exist-link"));
      fail("Disable link should fail as link doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }

  @Test
  public void testDisableJob() {
    // creaet job
    createJob("fromLink", "toLink", "jobName");

    // the job is enable
    MJob job = getClient().getJob("jobName");
    assertTrue(job.getEnabled());

    // disable job -name jobName
    Status status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-name", "jobName"));
    assertTrue(status != null && status == Status.OK);

    // the job is disable
    job = getClient().getJob("jobName");
    assertFalse(job.getEnabled());

    // disable the non-exist job
    try {
      execute(Arrays.asList(Constants.FN_JOB, "-name", "non-exist-job"));
      fail("Disable job should fail as job doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }
}
