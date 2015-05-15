/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.connector.ftp.configuration;

import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.ConfigValidationRunner;
import org.apache.sqoop.validation.Status;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;

/**
 * Unit tests for {@link org.apache.sqoop.connector.ftp.configuration.LinkConfiguration}.
 *
 * This uses MockFtpServer (http://mockftpserver.sourceforge.net/) to provide
 * a mock FTP server implementation.
 */
public class TestLinkConfiguration {

  private FakeFtpServer fakeFtpServer;
  private int port;
  private String username = "user";
  private String password = "pass";

  /**
   * Test valid configuration.
   */
  @Test
  public void testValidConfig() {
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;
    LinkConfiguration config = new LinkConfiguration();
    config.linkConfig.username = username;
    config.linkConfig.password = password;
    config.linkConfig.server = "localhost";
    config.linkConfig.port = port;
    result = runner.validate(config);
    Assert.assertTrue(result.getStatus() == Status.OK,
                      "Test of valid configuration failed");
  }

  /**
   * Test empty username.
   */
  @Test
  public void testEmptyUsername() {
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;
    LinkConfiguration config = new LinkConfiguration();
    config.linkConfig.username = "";
    config.linkConfig.password = password;
    config.linkConfig.server = "localhost";
    config.linkConfig.port = port;
    result = runner.validate(config);
    Assert.assertFalse(result.getStatus() == Status.OK,
                       "Test of empty username failed");
  }

  /**
   * Test invalid username.
   */
  @Test
  public void testInvalidUsername() {
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;
    LinkConfiguration config = new LinkConfiguration();
    config.linkConfig.username = "baduser";
    config.linkConfig.password = password;
    config.linkConfig.server = "localhost";
    config.linkConfig.port = port;
    result = runner.validate(config);
    Assert.assertFalse(result.getStatus() == Status.OK,
                       "Test of invalid username failed");
  }

  /**
   * Test empty server.
   */
  @Test
  public void TestEmptyServer() {
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;
    LinkConfiguration config = new LinkConfiguration();
    config.linkConfig.username = username;
    config.linkConfig.password = password;
    config.linkConfig.server = "";
    config.linkConfig.port = port;
    result = runner.validate(config);
    Assert.assertFalse(result.getStatus() == Status.OK,
                       "Test of empty server name failed");
  }

  /**
   * Create mock FTP server for testing, and add a user account for testing.
   */
  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.setServerControlPort(0);

    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry("/home/user/file.txt", "abcdef"));
    fakeFtpServer.setFileSystem(fileSystem);

    UserAccount userAccount = new UserAccount(username, password, "/");
    fakeFtpServer.addUserAccount(userAccount);

    fakeFtpServer.start();
    port = fakeFtpServer.getServerControlPort();
  }

  /**
   * Stop mock FTP server.
   */
  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    fakeFtpServer.stop();
  }
}
