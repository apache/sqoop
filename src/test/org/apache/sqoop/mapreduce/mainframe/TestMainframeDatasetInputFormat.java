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

package org.apache.sqoop.mapreduce.mainframe;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.mainframe.MainframeConfiguration;
import org.apache.sqoop.util.MainframeFTPClientUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.config.ConfigurationHelper;

public class TestMainframeDatasetInputFormat {

  private MainframeDatasetInputFormat<SqoopRecord> format;

  private FTPClient mockFTPClient;

  @Before
  public void setUp() {
    format = new MainframeDatasetInputFormat<SqoopRecord>();
    mockFTPClient = mock(FTPClient.class);
    MainframeFTPClientUtils.setMockFTPClient(mockFTPClient);
    try {
      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(true);
      when(mockFTPClient.completePendingCommand()).thenReturn(true);
      when(mockFTPClient.changeWorkingDirectory(anyString())).thenReturn(true);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
      when(mockFTPClient.getReplyString()).thenReturn("");
      when(mockFTPClient.noop()).thenReturn(200);
      when(mockFTPClient.setFileType(anyInt())).thenReturn(true);

      FTPFile ftpFile1 = new FTPFile();
      ftpFile1.setType(FTPFile.FILE_TYPE);
      ftpFile1.setName("test1");
      FTPFile ftpFile2 = new FTPFile();
      ftpFile2.setType(FTPFile.FILE_TYPE);
      ftpFile2.setName("test2");
      FTPFile[] ftpFiles = { ftpFile1, ftpFile2 };
      when(mockFTPClient.listFiles()).thenReturn(ftpFiles);
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }
  }

  @After
  public void tearDown() {
    MainframeFTPClientUtils.setMockFTPClient(null);
  }

  @Test
  public void testRetrieveDatasets() throws IOException {
    JobConf conf = new JobConf();
    conf.set(DBConfiguration.URL_PROPERTY, "localhost:12345");
    conf.set(DBConfiguration.USERNAME_PROPERTY, "user");
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
    // set the password in the secure credentials object
    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
        "pssword".getBytes());

    String dsName = "dsName1";
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME, dsName);
    Job job = new Job(conf);
    ConfigurationHelper.setJobNumMaps(job, 2);
    //format.getSplits(job);

    List<InputSplit> splits = new ArrayList<InputSplit>();
    splits = ((MainframeDatasetInputFormat<SqoopRecord>) format).getSplits(job);
    Assert.assertEquals("test1", ((MainframeDatasetInputSplit) splits.get(0))
        .getNextDataset().toString());
    Assert.assertEquals("test2", ((MainframeDatasetInputSplit) splits.get(1))
        .getNextDataset().toString());
  }
}
