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
import org.apache.sqoop.shell.ShellEnvironment;
import org.apache.sqoop.shell.SqoopCommand;
import org.apache.sqoop.shell.StatusCommand;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProviderForShellTest;
import org.apache.sqoop.test.testcases.ShellTestCase;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Infrastructure(dependencies = {SqoopInfrastructureProviderForShellTest.class, DatabaseInfrastructureProvider.class})
public class StatusCommandTest extends ShellTestCase {
  private ByteArrayOutputStream out;

  protected SqoopCommand createCommand(Groovysh shell) {
    return new StatusCommand(shell);
  }

  @Test
  public void testShowStatus() throws Exception {
    resetIO();

    createJob("fromLink", "toLink", "jobForStatusCommand");
    executeJob("jobForStatusCommand", false);

    Status status = (Status) execute(Arrays.asList(Constants.FN_JOB, "-name", "jobForStatusCommand"));
    assertTrue(status != null && status == Status.OK);
    String str = new String(out.toByteArray());
    assertTrue(str.contains("Submission details"));
    assertTrue(str.contains("Job Name: jobForStatusCommand"));
    assertTrue(str.contains("Server URL"));
    assertTrue(str.contains("Created by"));
    assertTrue(str.contains("Creation date"));
    assertTrue(str.contains("Lastly updated by"));
    assertTrue(str.contains("Submission details"));

    try {
      execute(Arrays.asList(Constants.FN_JOB, "-name", "non-exist-job"));
      fail("Show status should fail as job doesn't exist!");
    } catch (SqoopException e) {
      assertEquals(e.getErrorCode(), ClientError.CLIENT_0001);
    }
  }

  private void resetIO() {
    out = new ByteArrayOutputStream();
    ShellEnvironment.setIo(new IO(System.in, out, System.err));
  }
}
