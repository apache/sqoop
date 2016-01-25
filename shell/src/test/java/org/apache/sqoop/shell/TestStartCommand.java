/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.shell;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.Test;

public class TestStartCommand {
    StartCommand startCmd;
    SqoopClient client;

//    @BeforeTest(alwaysRun = false)
//    public void setup() {
//        Groovysh shell = new Groovysh();
//        startCmd = new StartCommand(shell);
//        ShellEnvironment.setInteractive(false);
//        ShellEnvironment.setIo(shell.getIo());
//        client = mock(SqoopClient.class);
//        ShellEnvironment.setClient(client);
//    }

    @Test(enabled = false)
    public void testStartJobSynchronousDisabled() throws InterruptedException {
        MSubmission submission = new MSubmission();
        when(client.startJob(any(String.class))).thenReturn(submission);

        // start job -name job_test
        Status status = (Status) startCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "job_test"));
        assertTrue(status != null && status == Status.OK);

        // Missing argument for name
        try {
            startCmd.execute(Arrays.asList(Constants.FN_JOB, "-name"));
            fail("Start job should fail as parameters aren't complete!");
        } catch (SqoopException e) {
            assertEquals(ShellError.SHELL_0003, e.getErrorCode());
            assertTrue(e.getMessage().contains("Missing argument for option"));
        }
    }

    @Test(enabled = false)
    public void testStartJobSynchronousEnabled() throws InterruptedException {
        when(client.startJob(any(String.class), any(SubmissionCallback.class), any(Long.class))).thenReturn(null);

        // start job -name job_test -synchronous
        Status status = (Status) startCmd.execute(Arrays.asList(Constants.FN_JOB, "-name", "job_test", "-synchronous"));
        assertTrue(status != null && status == Status.OK);
    }

}

