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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.util.MainframeFTPClientUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.LargeObjectLoader;

public class TestMainframeDatasetFTPRecordReader {

  private MainframeImportJob mfImportJob;

  private MainframeImportJob avroImportJob;

  private MainframeDatasetInputSplit mfDIS;

  private TaskAttemptContext context;

  private MainframeDatasetRecordReader mfDRR;

  private MainframeDatasetFTPRecordReader mfDFTPRR;

  private FTPClient mockFTPClient;

  public static class DummySqoopRecord extends SqoopRecord {
    private String field;

    public Map<String, Object> getFieldMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("fieldName", field);
      return map;
    }

    public void setField(String fieldName, Object fieldVal) {
      if (fieldVal instanceof String) {
        field = (String) fieldVal;
      }
    }

    public void setField(final String val) {
      this.field = val;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      field = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(field);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
      field = rs.getString(1);
    }

    @Override
    public void write(PreparedStatement s) throws SQLException {
      s.setString(1, field);
    }

    @Override
    public String toString() {
      return field;
    }

    @Override
    public int write(PreparedStatement stmt, int offset) throws SQLException {
      return 0;
    }

    @Override
    public String toString(DelimiterSet delimiters) {
      return null;
    }

    @Override
    public int getClassFormatVersion() {
      return 0;
    }

    @Override
    public int hashCode() {
      return Integer.parseInt(field);
    }

    public void loadLargeObjects(LargeObjectLoader loader) {
    }

    public void parse(CharSequence s) {
    }

    public void parse(Text s) {
    }

    public void parse(byte[] s) {
    }

    public void parse(char[] s) {
    }

    public void parse(ByteBuffer s) {
    }

    public void parse(CharBuffer s) {
    }

  }

  @Before
  public void setUp() throws IOException {
    mockFTPClient = mock(FTPClient.class);
    MainframeFTPClientUtils.setMockFTPClient(mockFTPClient);
    try {
      when(mockFTPClient.login("user", "pssword")).thenReturn(true);
      when(mockFTPClient.logout()).thenReturn(true);
      when(mockFTPClient.isConnected()).thenReturn(true);
      when(mockFTPClient.completePendingCommand()).thenReturn(true);
      when(mockFTPClient.changeWorkingDirectory(anyString())).thenReturn(true);
      when(mockFTPClient.getReplyCode()).thenReturn(200);
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

      when(mockFTPClient.retrieveFileStream("test1")).thenReturn(
          new ByteArrayInputStream("123\n456\n".getBytes()));
      when(mockFTPClient.retrieveFileStream("test2")).thenReturn(
          new ByteArrayInputStream("789\n".getBytes()));
      when(mockFTPClient.retrieveFileStream("NotComplete")).thenReturn(
          new ByteArrayInputStream("NotComplete\n".getBytes()));
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    }

    JobConf conf = new JobConf();
    conf.set(DBConfiguration.URL_PROPERTY, "localhost:" + "11111");
    conf.set(DBConfiguration.USERNAME_PROPERTY, "user");
    conf.set(DBConfiguration.PASSWORD_PROPERTY, "pssword");
    // set the password in the secure credentials object
    Text PASSWORD_SECRET_KEY = new Text(DBConfiguration.PASSWORD_PROPERTY);
    conf.getCredentials().addSecretKey(PASSWORD_SECRET_KEY,
        "pssword".getBytes());
    conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, DummySqoopRecord.class,
        DBWritable.class);

    Job job = new Job(conf);
    mfDIS = new MainframeDatasetInputSplit();
    mfDIS.addDataset("test1");
    mfDIS.addDataset("test2");
    context = mock(TaskAttemptContext.class);
    when(context.getConfiguration()).thenReturn(job.getConfiguration());
    mfDFTPRR = new MainframeDatasetFTPRecordReader();
  }

  @After
  public void tearDown() {
    try {
      mfDFTPRR.close();
    } catch (IOException ioe) {
      fail("Got IOException: " + ioe.toString());
    }
    MainframeFTPClientUtils.setMockFTPClient(null);
  }

  @Test
  public void testReadAllData() {
    try {
      mfDFTPRR.initialize(mfDIS, context);
      Assert.assertTrue("Retrieve of dataset", mfDFTPRR.nextKeyValue());
      Assert.assertEquals("Key should increase by records", 1, mfDFTPRR
          .getCurrentKey().get());
      Assert.assertEquals("Read value by line and by dataset", "123", mfDFTPRR
          .getCurrentValue().toString());
      Assert.assertEquals("Get progress according to left dataset",
          mfDFTPRR.getProgress(), (float) 0.5, 0.02);
      Assert.assertTrue("Retrieve of dataset", mfDFTPRR.nextKeyValue());
      Assert.assertEquals("Key should increase by records", 2, mfDFTPRR
          .getCurrentKey().get());
      Assert.assertEquals("Read value by line and by dataset", "456", mfDFTPRR
          .getCurrentValue().toString());
      Assert.assertEquals("Get progress according to left dataset",
          mfDFTPRR.getProgress(), (float) 0.5, 0.02);
      Assert.assertTrue("Retrieve of dataset", mfDFTPRR.nextKeyValue());
      Assert.assertEquals("Key should increase by records", 3, mfDFTPRR
          .getCurrentKey().get());
      Assert.assertEquals("Read value by line and by dataset", "789", mfDFTPRR
          .getCurrentValue().toString());
      Assert.assertEquals("Get progress according to left dataset",
          mfDFTPRR.getProgress(), (float) 1, 0.02);
      Assert.assertFalse("End of dataset", mfDFTPRR.nextKeyValue());
    } catch (IOException ioe) {
      fail("Got IOException: " + ioe.toString());
    } catch (InterruptedException ie) {
      fail("Got InterruptedException: " + ie.toString());
    }
  }

  @Test
  public void testReadPartOfData() {
    try {
      mfDFTPRR.initialize(mfDIS, context);
      Assert.assertTrue("Retrieve of dataset", mfDFTPRR.nextKeyValue());
      Assert.assertEquals("Key should increase by records", 1, mfDFTPRR
          .getCurrentKey().get());
      Assert.assertEquals("Read value by line and by dataset", "123", mfDFTPRR
          .getCurrentValue().toString());
      Assert.assertEquals("Get progress according to left dataset",
          mfDFTPRR.getProgress(), (float) 0.5, 0.02);
    } catch (IOException ioe) {
      fail("Got IOException: " + ioe.toString());
    } catch (InterruptedException ie) {
      fail("Got InterruptedException: " + ie.toString());
    }
  }

  @Test
  public void testFTPNotComplete() {
    try {
      mfDIS = new MainframeDatasetInputSplit();
      mfDIS.addDataset("NotComplete");
      mfDFTPRR.initialize(mfDIS, context);
      Assert.assertTrue("Retrieve of dataset", mfDFTPRR.nextKeyValue());
      when(mockFTPClient.completePendingCommand()).thenReturn(false);
      mfDFTPRR.nextKeyValue();
    } catch (IOException ioe) {
      Assert.assertEquals(
          "java.io.IOException: IOException during data transfer: "
              + "java.io.IOException: Failed to complete ftp command.",
          ioe.toString());
    } catch (InterruptedException ie) {
      fail("Got InterruptedException: " + ie.toString());
    }
  }
}
