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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;

public class TestMainframeDatasetBinaryRecord {

  private MainframeDatasetFTPRecordReader ftpRecordReader;
  private InputStream is;
  private FTPClient ftp;
  private final String DATASET_NAME = "dummy.ds";
  private final String DATASET_TYPE = "g";
  private static final Log LOG = LogFactory.getLog(
      TestMainframeDatasetBinaryRecord.class.getName());

  @Before
  public void setUp() throws IOException, InterruptedException {
    MainframeDatasetFTPRecordReader rdr = new MainframeDatasetFTPRecordReader();
    Configuration conf;
    MainframeDatasetInputSplit split;
    TaskAttemptContext context;
    ftpRecordReader = spy(rdr);
    is = mock(InputStream.class);
    ftp = mock(FTPClient.class);
    split = mock(MainframeDatasetInputSplit.class);
    context = mock(TaskAttemptContext.class);
    conf = new Configuration();
    when(ftp.retrieveFileStream(any(String.class))).thenReturn(is);
    when(ftp.changeWorkingDirectory(any(String.class))).thenReturn(true);
    doReturn("file1").when(ftpRecordReader).getNextDataset();
    when(split.getNextDataset()).thenReturn(DATASET_NAME);
    when(ftpRecordReader.getNextDataset()).thenReturn(DATASET_NAME);
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME,DATASET_NAME);
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,DATASET_TYPE);
    conf.setInt(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER_SIZE,MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE);
    ftpRecordReader.initialize(ftp, conf);
  }

  // Mock the inputstream.read method and manipulate the function parameters
  protected Answer returnSqoopRecord(final int byteLength) {
    return new Answer() {
      public Object answer(InvocationOnMock invocation) {
        return byteLength;
      }
    };
  }

  @Test
  public void testGetNextBinaryRecordForFullRecord() {

    MainframeDatasetBinaryRecord record = new MainframeDatasetBinaryRecord();
    try {
      when(is.read(any(byte[].class),anyInt(),anyInt()))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE))
        .thenReturn(-1);
      when(ftp.completePendingCommand()).thenReturn(true);
      Assert.assertTrue(ftpRecordReader.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertEquals(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE.intValue(),((byte[])record.getFieldMap().values().iterator().next()).length);
    } catch (IOException ioe) {
      LOG.error("Issue with reading 1 full binary buffer record", ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Test
  public void testGetNextBinaryRecordForPartialRecord() {
    int expectedBytesRead = 10;
    MainframeDatasetBinaryRecord record = new MainframeDatasetBinaryRecord();
    try {
      when(is.read(any(byte[].class),anyInt(),anyInt()))
        .thenAnswer(returnSqoopRecord(10))
        .thenReturn(-1);
      when(ftp.completePendingCommand()).thenReturn(true);
      Assert.assertTrue(ftpRecordReader.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertEquals(expectedBytesRead,(((byte[])record.getFieldMap().values().iterator().next()).length));
    } catch (IOException ioe) {
      LOG.error("Issue with reading 10 byte binary record", ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Test
  public void testGetNextBinaryRecordFor2Records() {
    // test 1 full record, and 1 partial
    int expectedBytesRead = 10;
    MainframeDatasetBinaryRecord record = new MainframeDatasetBinaryRecord();
    try {
      when(is.read(any(byte[].class),anyInt(),anyInt()))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE))
        .thenAnswer(returnSqoopRecord(10))
        .thenReturn(-1);
      when(ftp.completePendingCommand()).thenReturn(true);
      Assert.assertTrue(ftpRecordReader.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertTrue(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE.equals((((byte[])record.getFieldMap().values().iterator().next()).length)));
      record = new MainframeDatasetBinaryRecord();
      Assert.assertTrue(ftpRecordReader.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertEquals(expectedBytesRead,(((byte[])record.getFieldMap().values().iterator().next()).length));
    } catch (IOException ioe) {
      LOG.error("Issue with reading 1 full binary buffer record followed by 1 partial binary buffer record", ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Test
  public void testGetNextBinaryRecordForMultipleReads() {
    // test reading 1 record where the stream returns less than a full buffer
    MainframeDatasetBinaryRecord record = new MainframeDatasetBinaryRecord();
    try {
      when(is.read(any(byte[].class),anyInt(),anyInt()))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE /2))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE /2))
        .thenReturn(-1);
      when(ftp.completePendingCommand()).thenReturn(true);
      Assert.assertTrue(ftpRecordReader.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertEquals(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE.intValue(),((byte[])record.getFieldMap().values().iterator().next()).length);
      record = new MainframeDatasetBinaryRecord();
      Assert.assertFalse(ftpRecordReader.getNextBinaryRecord(record));
      Assert.assertNull((((byte[])record.getFieldMap().values().iterator().next())));
    } catch (IOException ioe) {
      LOG.error("Issue with verifying reading partial buffer binary records", ioe);
      throw new RuntimeException(ioe);
    }
  }
}
