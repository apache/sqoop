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
package org.apache.sqoop.test.testcases;

import jline.console.ConsoleReader;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.shell.ShellEnvironment;
import org.apache.sqoop.shell.SqoopCommand;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

abstract public class ShellTestCase extends SqoopTestCase {

  protected SqoopCommand command = null;
  protected ConsoleReader reader = null;
  protected ByteArrayInputStream in = null;
  protected byte[] data = null;
  private final static String TEST_CONNECTOR_NAME = "test-connector-for-shell";

  @BeforeMethod
  public void setup() throws Exception {
    Groovysh shell = new Groovysh();
    command = createCommand(shell);
    ShellEnvironment.setIo(shell.getIo());
    ShellEnvironment.setClient(getClient());

    data = new byte[1024];
    in = new ByteArrayInputStream(data);
    reader = new ConsoleReader(in, System.out);
    ShellEnvironment.setConsoleReader(reader);
  }

  @AfterMethod
  public void cleanup() throws IOException {
    clearJob();
    clearLink();
  }

  protected Object execute(List args) {
    return command.execute(args);
  }

  protected void initData(String destData) throws UnsupportedEncodingException {
    byte[] destDataBytes = destData.getBytes("UTF-8");
    System.arraycopy(destDataBytes, 0, data, 0, destDataBytes.length);
    in.reset();
  }

  protected void initEnv() {
    in.reset();
    for (int i = 0; i < data.length; i++) {
      data[i] = '\0';
    }
  }

  protected MLink createLink(String linkName) {
    return createLink(linkName, TEST_CONNECTOR_NAME);
  }

  protected void createJob(String fromLinkName, String toLinkName, String jobName) {
    // create link for test
    createLink(fromLinkName, TEST_CONNECTOR_NAME);
    createLink(toLinkName, TEST_CONNECTOR_NAME);
    MJob mjob = getClient().createJob(fromLinkName, toLinkName);
    mjob.setName(jobName);
    saveJob(mjob);
  }

  // the method should be override by sub class
  abstract protected SqoopCommand createCommand(Groovysh shell);
}
