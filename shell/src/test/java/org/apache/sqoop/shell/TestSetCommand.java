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

import java.util.Arrays;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.validation.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSetCommand {
  SetCommand setCmd;
  SqoopClient client;

//  @BeforeTest(alwaysRun = false)
//  public void setup() {
//    Groovysh shell = new Groovysh();
//    setCmd = new SetCommand(shell);
//    ShellEnvironment.setInteractive(false);
//    ShellEnvironment.setIo(shell.getIo());
//    client = new SqoopClient(StringUtils.EMPTY);
//    ShellEnvironment.setClient(client);
//  }

  @Test(enabled = false)
  public void testSetServer() {
    ShellEnvironment.cleanup();
    // set server -url http://host-test:7070/sqoop-test
    Status status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-url", "http://host-test:7070/sqoop-test"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://host-test:7070/sqoop-test/");

    // use the default webapp path if not specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-url", "http://host-test:7070/"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertTrue(client.getServerUrl().equals("http://host-test:7070/sqoop/"));

    // use the default webapp and port if not specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-url", "http://host-test/"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertTrue(client.getServerUrl().equals("http://host-test:12000/sqoop/"));

    // option host is ignored when option url is specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-url", "http://host-test:7070/sqoop-test", "-host", "host2-test"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://host-test:7070/sqoop-test/");

    // option port is ignored when option url is specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-url", "http://host-test:7070/sqoop-test", "-port", "12000"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://host-test:7070/sqoop-test/");

    // option webapp is ignored when option url is specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-url", "http://host-test:7070/sqoop-test", "-webapp", "sqoop2-test"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://host-test:7070/sqoop-test/");

    // Missing argument for option url
    try {
      status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-url"));
      Assert.fail("Set server should fail as url is missing!");
    } catch (SqoopException e) {
      Assert.assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("Missing argument for option"));
    }
  }

  @Test(enabled = false)
  public void testSetServerWithoutOptionURL() {
    ShellEnvironment.cleanup();
    // use option host, port, webapp when option url is not specified
    Status status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-host", "host2-test", "-port", "7070", "-webapp", "sqoop2-test"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://host2-test:7070/sqoop2-test/");

    ShellEnvironment.cleanup();
    // use default host if option host is not specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-port", "7070", "-webapp", "sqoop2-test"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://localhost:7070/sqoop2-test/");

    ShellEnvironment.cleanup();
    // use default port if option port is not specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-host", "host2-test", "-webapp", "sqoop2-test"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://host2-test:12000/sqoop2-test/");

    ShellEnvironment.cleanup();
    // use default webapp if option webapp is not specified
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_SERVER, "-host", "host2-test", "-port", "7070"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(client.getServerUrl(), "http://host2-test:7070/sqoop/");
  }

  @Test(enabled = false)
  public void testSetOption() {
    // set option -name verbose -value true
    Status status = (Status) setCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "verbose", "-value", "true"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertTrue(ShellEnvironment.isVerbose());

    // set option -name verbose -value 1
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "verbose", "-value", "1"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertTrue(ShellEnvironment.isVerbose());

    // set option -name verbose -value 0
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "verbose", "-value", "0"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertFalse(ShellEnvironment.isVerbose());

    // set option -name poll-timeout -value 12345
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "poll-timeout", "-value", "12345"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(ShellEnvironment.getPollTimeout(), 12345);

    // when value of poll-timeout is not number, poll-timeout should stay the old value
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "poll-timeout", "-value", "abc"));
    Assert.assertTrue(status != null && status == Status.OK);
    Assert.assertEquals(ShellEnvironment.getPollTimeout(), 12345);

    // skip non exist options, options already set should stay the old value
    status = (Status) setCmd.execute(Arrays.asList(Constants.FN_OPTION, "-name", "non-exist-option", "-value", "opt-value"));
    Assert.assertTrue(status == null);
    Assert.assertFalse(ShellEnvironment.isVerbose());
    Assert.assertEquals(ShellEnvironment.getPollTimeout(), 12345);
  }
}
