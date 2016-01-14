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
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.shell.SqoopCommand;
import org.apache.sqoop.shell.StartCommand;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProviderForShellTest;
import org.apache.sqoop.test.testcases.ShellTestCase;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Infrastructure(dependencies = {SqoopInfrastructureProviderForShellTest.class, DatabaseInfrastructureProvider.class})
public class StartCommandTest extends ShellTestCase {

  protected SqoopCommand createCommand(Groovysh shell) {
    return new StartCommand(shell);
  }

  @Test
  public void testStartJob() throws Exception {
    createJob("fromLink1", "toLink1", "jobForTestStartJob1");
    Status status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-name", "jobForTestStartJob1"));
    assertTrue(status != null && status == Status.OK);

    List<MSubmission> submissions = getClient().getSubmissionsForJob("jobForTestStartJob1");
    assertEquals(submissions.size(), 1);

    try {
      execute(Arrays.asList(Constants.FN_JOB, "-name", "non-exist-job"));
      fail("Start job should fail as job doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }

  @Test
  public void testStartJobWithSyn() throws Exception {
    createJob("fromLink2", "toLink2", "jobForTestStartJob2");
    // start job with synchronous
    Status status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-name", "jobForTestStartJob2", "-synchronous"));
    assertTrue(status != null && status == Status.OK);

    List<MSubmission> submissions = getClient().getSubmissionsForJob("jobForTestStartJob2");
    assertEquals(submissions.size(), 1);
    assertTrue(submissions.get(0).getStatus() == SubmissionStatus.FAILED);
  }
}
