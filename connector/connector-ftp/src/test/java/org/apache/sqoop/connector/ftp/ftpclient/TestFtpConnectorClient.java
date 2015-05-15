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
package org.apache.sqoop.connector.ftp.ftpclient;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.etl.io.DataReader;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;

/**
 * Unit tests for {@link org.apache.sqoop.connector.ftp.ftpclient.FtpConnectorClient} class.
 *
 * This uses MockFtpServer (http://mockftpserver.sourceforge.net/) to provide
 * a mock FTP server implementation.
 *
 * Note that this duplicates other tests currently, but leaving for now in case
 * additional functionality is added to FtpConnectorClient.
 */
public class TestFtpConnectorClient {

  private FakeFtpServer fakeFtpServer;
  private int port;
  private String username = "user";
  private String password = "pass";

  /**
   * Test connect and login to FTP server.
   */
  @Test
  public void testValidLogin() {
    try {
      FtpConnectorClient client = new FtpConnectorClient("localhost", port);
      client.connect(username, password);
      client.disconnect();
    } catch (SqoopException e) {
      Assert.fail("login failed " + e.getMessage());
    }
  }

  /**
   * Test invalid login to FTP server.
   */
  @Test
  public void testInvalidLogin() {
    try {
      FtpConnectorClient client = new FtpConnectorClient("localhost", port);
      client.connect("baduser", "badpass");
      client.disconnect();
      Assert.fail("expected exception for invalid login");
    } catch (SqoopException e) {
      // Expected
    }
  }

  /**
   * Test streaming upload.
   */
  @Test
  public void testUploadStream() {

    final int NUMBER_OF_ROWS = 1000;

    DataReader reader = new DataReader() {
      private long index = 0L;
      @Override
      public Object[] readArrayRecord() {
        return null;
     }

      @Override
      public String readTextRecord() {
        if (index++ < NUMBER_OF_ROWS) {
          return index + "," + (double)index + ",'" + index + "'";
        } else {
          return null;
        }
      }

      @Override
      public Object readContent() {
        return null;
      }
    };

    try {
      FtpConnectorClient client = new FtpConnectorClient("localhost", port);
      client.connect(username, password);
      long rowsWritten = client.uploadStream(reader, "/uploads/test.txt");
      client.disconnect();
      Assert.assertTrue(rowsWritten == NUMBER_OF_ROWS,
                        ("actual rows written=" + rowsWritten + " instead of " +
                         NUMBER_OF_ROWS));
    } catch(Exception e) {
      Assert.fail("caught exception: " + e.getMessage());
    }
  }

  /**
   * Create mock FTP server for testing, and add a user account for testing.
   */
  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.setServerControlPort(0);

    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new DirectoryEntry("/uploads"));
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
