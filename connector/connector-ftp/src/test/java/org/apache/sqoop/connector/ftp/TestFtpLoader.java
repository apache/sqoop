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
package org.apache.sqoop.connector.ftp;

import org.apache.sqoop.connector.ftp.configuration.LinkConfiguration;
import org.apache.sqoop.connector.ftp.configuration.ToJobConfiguration;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.LoaderContext;

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
 * Unit tests for {@link org.apache.sqoop.connector.ftp.Loader} class.
 *
 * This uses MockFtpServer (http://mockftpserver.sourceforge.net/) to provide
 * a mock FTP server implementation.
 */
public class TestFtpLoader {

  private FakeFtpServer fakeFtpServer;
  private int port;
  private String username = "user";
  private String password = "pass";
  private FtpLoader loader;

  @Test
  public void testLoader() {

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
      LoaderContext context = new LoaderContext(null, reader, null, "test_user");
      LinkConfiguration linkConfig = new LinkConfiguration();
      linkConfig.linkConfig.username = username;
      linkConfig.linkConfig.password = password;
      linkConfig.linkConfig.server = "localhost";
      linkConfig.linkConfig.port = port;
      ToJobConfiguration jobConfig = new ToJobConfiguration();
      jobConfig.toJobConfig.outputDirectory = "uploads";
      loader.load(context, linkConfig, jobConfig);
      Long rowsWritten = loader.getRowsWritten();
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

    loader = new FtpLoader();

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
