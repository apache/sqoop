package org.apache.sqoop.mapreduce.mainframe;

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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMainframeDatasetBinaryRecord {

  private MainframeDatasetFTPRecordReader mfDFTPRR;
  private InputStream is;
  private FTPClient ftp;
  private Configuration conf;
  private MainframeDatasetInputSplit split;
  private TaskAttemptContext context;
  final String DATASET_NAME = "dummy.ds";
  final String DATASET_TYPE = "g";

  @Before
  public void setUp() throws IOException, InterruptedException {
    mfDFTPRR = new MainframeDatasetFTPRecordReader();
    is = mock(InputStream.class);
    ftp = mock(FTPClient.class);
    split = mock(MainframeDatasetInputSplit.class);
    context = mock(TaskAttemptContext.class);
    conf = new Configuration();
    when(ftp.retrieveFileStream(any(String.class))).thenReturn(is);
    when(ftp.changeWorkingDirectory(any(String.class))).thenReturn(true);
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME,DATASET_NAME);
    conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,DATASET_TYPE);
    mfDFTPRR.initialize(split, context, ftp, conf);
  }

  // Mock the inputstream.read method and manipulate the function parameters
  protected Answer returnSqoopRecord(final int byteLength) {
    return new Answer() {
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        byte[] bytes = args[0] instanceof byte[] ? ((byte[]) args[0]) : null;
        int len = args[2] instanceof Integer ? ((int) args[2]) : null;
        bytes = new byte[byteLength];
        return byteLength;
      }
    };
  }

  @Test
  public void testGetNextBinaryRecordForFullRecord() {

    MainframeDatasetBinaryRecord record = new MainframeDatasetBinaryRecord();
    try {
      when(is.read(any(byte[].class),anyInt(),anyInt()))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER))
        .thenReturn(-1);
      when(ftp.completePendingCommand()).thenReturn(true);
      Assert.assertTrue(mfDFTPRR.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertTrue(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER.equals(((byte[])record.getFieldMap().values().iterator().next()).length));
    } catch (IOException ioe) {
      fail ("Got IOException: "+ ioe);
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
      Assert.assertTrue(mfDFTPRR.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertEquals(expectedBytesRead,(((byte[])record.getFieldMap().values().iterator().next()).length));
    } catch (IOException ioe) {
      fail ("Got IOException: "+ ioe);
    }
  }

  @Test
  public void testGetNextBinaryRecordFor2Records() {
    // test 1 full record, and 1 partial
    int expectedBytesRead = 10;
    MainframeDatasetBinaryRecord record = new MainframeDatasetBinaryRecord();
    try {
      when(is.read(any(byte[].class),anyInt(),anyInt()))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER))
        .thenAnswer(returnSqoopRecord(10))
        .thenReturn(-1);
      when(ftp.completePendingCommand()).thenReturn(true);
      Assert.assertTrue(mfDFTPRR.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertTrue(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER.equals((((byte[])record.getFieldMap().values().iterator().next()).length)));
      record = new MainframeDatasetBinaryRecord();
      Assert.assertTrue(mfDFTPRR.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertEquals(expectedBytesRead,(((byte[])record.getFieldMap().values().iterator().next()).length));
    } catch (IOException ioe) {
      fail ("Got IOException: "+ ioe);
    }
  }

  @Test
  public void testGetNextBinaryRecordForMultipleReads() {
    // test reading 1 record where the stream returns less than a full buffer
    MainframeDatasetBinaryRecord record = new MainframeDatasetBinaryRecord();
    try {
      when(is.read(any(byte[].class),anyInt(),anyInt()))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER/2))
        .thenAnswer(returnSqoopRecord(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER/2))
        .thenReturn(-1);
      when(ftp.completePendingCommand()).thenReturn(true);
      Assert.assertTrue(mfDFTPRR.getNextBinaryRecord(record));
      Assert.assertFalse(record.getFieldMap().values().isEmpty());
      Assert.assertTrue(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER.equals((((byte[])record.getFieldMap().values().iterator().next()).length)));
      record = new MainframeDatasetBinaryRecord();
      Assert.assertFalse(mfDFTPRR.getNextBinaryRecord(record));
      Assert.assertNull((((byte[])record.getFieldMap().values().iterator().next())));
    } catch (IOException ioe) {
      fail ("Got IOException: "+ ioe);
    }
  }
}
