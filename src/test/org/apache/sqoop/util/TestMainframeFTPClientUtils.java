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

package org.apache.sqoop.util;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import java.util.List;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.mapreduce.JobBase;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.mainframe.MainframeConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMainframeFTPClientUtils {

  private JobConf conf;

  private FTPClient mockFTPClient;

  @Before
  public void setUp() {
    conf = new JobConf();
    mockFTPClient = mock(FTPClient.class);
    when(mockFTPClient.getReplyString()).thenReturn("");
    MainframeFTPClientUtils.setMockFTPClient(mockFTPClient);
  }

  @After
  public void tearDown() {
    MainframeFTPClientUtils.setMockFTPClient(null);
  }

  @Test
  public void testAnonymous_VERBOSE_IllegelPort() {
    try {
      when(mockFTPClient.login("anonymous", "")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(false);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }

    conf.set(DBConfiguration.URL_PROPERTY, "localhost:testPort");
    conf.setBoolean(JobBase.PROPERTY_VERBOSE, true);

    FTPClient ftp = null;
    boolean success = false;
    try {
      ftp = MainframeFTPClientUtils.getFTPConnection(conf);
    } catch (IOException ioe) {
      fail("No IOException should be thrown!");
    } finally {
      success = MainframeFTPClientUtils.closeFTPConnection(ftp);
    }
    Assert.assertTrue(success);
  }

  @Test
  public void testCannotConnect() {
    try {
      when(mockFTPClient.login("testUser", "")).thenReturn(false);
    } catch (IOException ioe) {
      fail("No IOException should be thrown!");
    }

    conf.set(DBConfiguration.URL_PROPERTY, "testUser:11111");
    try {
      MainframeFTPClientUtils.getFTPConnection(conf);
    } catch (IOException ioe) {
      Assert.assertEquals(
          "java.io.IOException: FTP server testUser refused connection:",
          ioe.toString());
    }
  }

  @Test
  public void testWrongUsername() {
    try {
      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(false);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }

    FTPClient ftp = null;
    conf.set(DBConfiguration.URL_PROPERTY, "localhost:11111");
    conf.set(DBConfiguration.USERNAME_PROPERTY, "userr");
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
    // set the password in the secure credentials object
    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
        "pssword".getBytes());

    try {
      ftp = MainframeFTPClientUtils.getFTPConnection(conf);
    } catch (IOException ioe) {
      Assert.assertEquals(
          "java.io.IOException: Could not login to server localhost:",
          ioe.toString());
    }
    Assert.assertNull(ftp);
  }

  @Test
  public void testNotListDatasets() {
    try {
      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(false);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }

    conf.set(DBConfiguration.URL_PROPERTY, "localhost:11111");
    conf.set(DBConfiguration.USERNAME_PROPERTY, "userr");
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
    // set the password in the secure credentials object
    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
        "pssword".getBytes());

    try {
      MainframeFTPClientUtils.listSequentialDatasets("pdsName", conf);
    } catch (IOException ioe) {
      Assert.assertEquals("java.io.IOException: "
          + "Could not list datasets from pdsName:"
          + "java.io.IOException: Could not login to server localhost:",
          ioe.toString());
    }
  }

  @Test
  public void testListSequentialDatasets() {
    try {
      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(false);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
      when(mockFTPClient.changeWorkingDirectory("a.b.c")).thenReturn(true);
        FTPFile file1 = new FTPFile();
        file1.setName("blah1");
        file1.setType(FTPFile.FILE_TYPE);
        FTPFile file2 = new FTPFile();
        file2.setName("blah2");
        file2.setType(FTPFile.FILE_TYPE);
      when(mockFTPClient.listFiles()).thenReturn(new FTPFile[] {file1,file2});
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }

    conf.set(DBConfiguration.URL_PROPERTY, "localhost:11111");
    conf.set(DBConfiguration.USERNAME_PROPERTY, "user");
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,"s");
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME,"a.b.c.blah1");
    // set the password in the secure credentials object
    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
            "pssword".getBytes());

    try {
      List<String> files = MainframeFTPClientUtils.listSequentialDatasets("a.b.c.blah1", conf);
        Assert.assertEquals(1,files.size());
    } catch (IOException ioe) {
		fail("No IOException should be thrown!");
    }
  }

  @Test
  public void testListEmptySequentialDatasets() {
    try {
      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(false);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
      when(mockFTPClient.changeWorkingDirectory("a.b.c")).thenReturn(true);
        FTPFile file1 = new FTPFile();
        file1.setName("blah0");
        file1.setType(FTPFile.FILE_TYPE);
        FTPFile file2 = new FTPFile();
        file2.setName("blah2");
        file2.setType(FTPFile.FILE_TYPE);
      when(mockFTPClient.listFiles()).thenReturn(new FTPFile[]{file1,file2});
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }

    conf.set(DBConfiguration.URL_PROPERTY, "localhost:11111");
    conf.set(DBConfiguration.USERNAME_PROPERTY, "user");
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,"s");
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME,"a.b.c.blah1");
    // set the password in the secure credentials object
    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
            "pssword".getBytes());

    try {
		String dsName = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
		List<String> files = MainframeFTPClientUtils.listSequentialDatasets(dsName, conf);
        Assert.assertEquals(0,files.size());
    } catch (IOException ioe) {
		fail("No IOException should be thrown!");
    }
  }

  @Test
  public void testTrailingDotSequentialDatasets() {
    try {
      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(false);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
      when(mockFTPClient.changeWorkingDirectory("'a.b.c.blah1'")).thenThrow(new IOException("Folder not found"));
        FTPFile file1 = new FTPFile();
        file1.setName("blah1");
        file1.setType(FTPFile.FILE_TYPE);
        FTPFile file2 = new FTPFile();
        file2.setName("blah2");
        file2.setType(FTPFile.FILE_TYPE);
      when(mockFTPClient.listFiles()).thenReturn(new FTPFile[]{file1,file2});
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }

    conf.set(DBConfiguration.URL_PROPERTY, "localhost:11111");
    conf.set(DBConfiguration.USERNAME_PROPERTY, "user");
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,"s");
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME,"a.b.c.blah1.");
    // set the password in the secure credentials object
    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
            "pssword".getBytes());

    try {
		String dsName = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
		List<String> files = MainframeFTPClientUtils.listSequentialDatasets(dsName, conf);
        Assert.assertEquals(0,files.size());
    } catch (IOException ioe) {
		String ioeString = ioe.getMessage();
		Assert.assertEquals("Could not list datasets from a.b.c.blah1:java.io.IOException: Folder not found",ioeString);
    }
  }
  
  @Test
  public void testGdgGetLatest() {
	  try {
	      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
	      when(mockFTPClient.logout()).thenReturn(true);
	      when(mockFTPClient.isConnected()).thenReturn(false);
	      when(mockFTPClient.getReplyCode()).thenReturn(200);
	      when(mockFTPClient.changeWorkingDirectory("'a.b.c'")).thenReturn(true);
	        FTPFile file1 = new FTPFile();
	        file1.setName("G0100V00");
	        file1.setType(FTPFile.FILE_TYPE);
	        FTPFile file2 = new FTPFile();
	        file2.setName("G0101V00");
	        file2.setType(FTPFile.FILE_TYPE);
	      when(mockFTPClient.listFiles()).thenReturn(new FTPFile[]{file1,file2});
	    } catch (IOException e) {
	      fail("No IOException should be thrown!");
	    }

	    conf.set(DBConfiguration.URL_PROPERTY, "localhost:11111");
	    conf.set(DBConfiguration.USERNAME_PROPERTY, "user");
	    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
	    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,"g");
	    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME,"a.b.c.d");
	    // set the password in the secure credentials object
	    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
	    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
	            "pssword".getBytes());

	    try {
			String dsName = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
			List<String> files = MainframeFTPClientUtils.listSequentialDatasets(dsName, conf);
	        Assert.assertEquals("G0101V00", files.get(0));
			Assert.assertEquals(1,files.size());
	    } catch (IOException ioe) {
			String ioeString = ioe.getMessage();
			Assert.assertEquals("Could not list datasets from a.b.c.blah1:java.io.IOException: Folder not found",ioeString);
	    }
  }
}
