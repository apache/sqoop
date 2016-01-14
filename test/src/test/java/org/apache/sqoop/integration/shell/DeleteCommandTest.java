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
import org.apache.sqoop.shell.DeleteCommand;
import org.apache.sqoop.shell.SqoopCommand;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProviderForShellTest;
import org.apache.sqoop.test.testcases.ShellTestCase;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Infrastructure(dependencies = {SqoopInfrastructureProviderForShellTest.class, DatabaseInfrastructureProvider.class})
public class DeleteCommandTest extends ShellTestCase {

  protected SqoopCommand createCommand(Groovysh shell) {
    return new DeleteCommand(shell);
  }

  @Test
  public void testDeleteLink() {
    // creaet link
    createLink("linkName");

    // delete link -name linkName
    Status status = (Status) execute(Arrays.asList(Constants.FN_LINK, "-name", "linkName"));
    Assert.assertTrue(status != null && status == Status.OK);

    // verify the link is deleted
    try {
      getClient().getLink("linkName");
      fail("Link doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }

    // delete the non-exist link
    try {
      execute(Arrays.asList(Constants.FN_LINK, "-name", "non-exist-link"));
      fail("Delete link should fail as link doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }

  @Test
  public void testDeleteJob() {
    // creaet job
    createJob("fromLink", "toLink", "jobName");

    // delete job -name jobName
    Status status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-name", "jobName"));
    Assert.assertTrue(status != null && status == Status.OK);

    // verify the job is deleted
    try {
      getClient().getJob("jobName");
      fail("Job doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }

    // delete the non-exist job
    try {
      execute(Arrays.asList(Constants.FN_JOB, "-name", "non-exist-job"));
      fail("Delete job should fail as job doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }
}
