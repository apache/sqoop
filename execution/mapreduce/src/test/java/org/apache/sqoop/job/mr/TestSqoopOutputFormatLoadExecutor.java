/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.job.mr;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.CSVIntermediateDataFormat;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.util.MRJobTestUtil;
import org.apache.sqoop.schema.NullSchema;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.submission.counter.SqoopCounters;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ConcurrentModificationException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

public class TestSqoopOutputFormatLoadExecutor {

  private Configuration conf;
  private TaskAttemptContext jobContextMock;

  public static class ThrowingLoader extends Loader<Object, Object> {

    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      context.getDataReader().readTextRecord();
      throw new BrokenBarrierException();
    }

    @Override
    public long getRowsWritten() {
      return 0;
    }
  }

  public static class ThrowingContinuousLoader extends Loader<Object, Object> {

    private long rowsWritten = 0;
    public ThrowingContinuousLoader() {
    }

    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      Object o;
      String[] arr;
      while ((o = context.getDataReader().readTextRecord()) != null) {
        arr = o.toString().split(",");
        Assert.assertEquals(100, arr.length);
        for (int i = 0; i < arr.length; i++) {
          Assert.assertEquals(i, Integer.parseInt(arr[i]));
        }
        rowsWritten++;
        if (rowsWritten == 5) {
          throw new ConcurrentModificationException();
        }
      }
    }

    @Override
    public long getRowsWritten() {
      return rowsWritten;
    }
  }

  public static class GoodLoader extends Loader<Object, Object> {
    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      String[] arr = context.getDataReader().readTextRecord().toString().split(",");
      Assert.assertEquals(100, arr.length);
      for (int i = 0; i < arr.length; i++) {
        Assert.assertEquals(i, Integer.parseInt(arr[i]));
      }
    }

    @Override
    public long getRowsWritten() {
      return 0;
    }
  }

  public static class GoodContinuousLoader extends Loader<Object, Object> {

    private long rowsWritten = 0;

    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      int rowsWritten = 0;
      Object o;
      String[] arr;
      while ((o = context.getDataReader().readTextRecord()) != null) {
        arr = o.toString().split(",");
        Assert.assertEquals(100, arr.length);
        for (int i = 0; i < arr.length; i++) {
          Assert.assertEquals(i, Integer.parseInt(arr[i]));
        }
        rowsWritten++;
      }
      Assert.assertEquals(10, rowsWritten);
    }

    @Override
    public long getRowsWritten() {
      return rowsWritten;
    }
  }

  // TODO:SQOOP-1873: Mock objects instead
  private Matcher getMatcher(){
    return MatcherFactory.getMatcher(NullSchema.getInstance(),
        NullSchema.getInstance());

  }
  // TODO:SQOOP-1873: Mock objects instead
  private IntermediateDataFormat<?> getIDF() {
    return new CSVIntermediateDataFormat(getSchema());
  }

  private Schema getSchema() {
    return new Schema("test").addColumn(new Text("t"));
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    conf = new Configuration();
    conf.setIfUnset(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT,
        CSVIntermediateDataFormat.class.getName());
    jobContextMock = mock(TaskAttemptContext.class);
    GenericCounter counter = new GenericCounter("test", "test-me");
    when(((TaskAttemptContext) jobContextMock).getCounter(SqoopCounters.ROWS_WRITTEN)).thenReturn(counter);
    org.apache.hadoop.mapred.JobConf testConf = new org.apache.hadoop.mapred.JobConf();
    when(jobContextMock.getConfiguration()).thenReturn(testConf);
  }

  @Test(expectedExceptions = BrokenBarrierException.class)
  public void testWhenLoaderThrows() throws Throwable {
    conf.set(MRJobConstants.JOB_ETL_LOADER, ThrowingLoader.class.getName());
    SqoopOutputFormatLoadExecutor executor = new SqoopOutputFormatLoadExecutor(jobContextMock,
        ThrowingLoader.class.getName(), getIDF(), getMatcher());
    RecordWriter<SqoopWritable, NullWritable> writer = executor
        .getRecordWriter();
    IntermediateDataFormat<?> dataFormat = MRJobTestUtil.getTestIDF();
    SqoopWritable writable = new SqoopWritable(dataFormat);
    try {
      for (int count = 0; count < 100; count++) {
        dataFormat.setCSVTextData(String.valueOf(count));
        writer.write(writable, null);
      }
    } catch (SqoopException ex) {
      throw ex.getCause();
    }
  }

  @Test
  public void testSuccessfulContinuousLoader() throws Throwable {
    conf.set(MRJobConstants.JOB_ETL_LOADER, GoodContinuousLoader.class.getName());

    SqoopOutputFormatLoadExecutor executor = new SqoopOutputFormatLoadExecutor(jobContextMock,
        GoodContinuousLoader.class.getName(), getIDF(), getMatcher());
    RecordWriter<SqoopWritable, NullWritable> writer = executor.getRecordWriter();
    IntermediateDataFormat<?> dataFormat = MRJobTestUtil.getTestIDF();
    SqoopWritable writable = new SqoopWritable(dataFormat);
    for (int i = 0; i < 10; i++) {
      StringBuilder builder = new StringBuilder();
      for (int count = 0; count < 100; count++) {
        builder.append(String.valueOf(count));
        if (count != 99) {
          builder.append(",");
        }
      }
      dataFormat.setCSVTextData(builder.toString());
      writer.write(writable, null);
    }
    writer.close(null);
    verify(jobContextMock, times(1)).getConfiguration();
    verify(jobContextMock, times(1)).getCounter(SqoopCounters.ROWS_WRITTEN);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testSuccessfulLoader() throws Throwable {
    SqoopOutputFormatLoadExecutor executor = new SqoopOutputFormatLoadExecutor(jobContextMock,
        GoodLoader.class.getName(), getIDF(), getMatcher());
    RecordWriter<SqoopWritable, NullWritable> writer = executor
        .getRecordWriter();
    IntermediateDataFormat<?> dataFormat = MRJobTestUtil.getTestIDF();
    SqoopWritable writable = new SqoopWritable(dataFormat);
    StringBuilder builder = new StringBuilder();
    for (int count = 0; count < 100; count++) {
      builder.append(String.valueOf(count));
      if (count != 99) {
        builder.append(",");
      }
    }
    dataFormat.setCSVTextData(builder.toString());
    writer.write(writable, null);

    // Allow writer to complete.
    TimeUnit.SECONDS.sleep(5);
    writer.close(null);
    verify(jobContextMock, times(1)).getConfiguration();
    verify(jobContextMock, times(1)).getCounter(SqoopCounters.ROWS_WRITTEN);
  }

  @Test(expectedExceptions = ConcurrentModificationException.class)
  public void testThrowingContinuousLoader() throws Throwable {
    conf.set(MRJobConstants.JOB_ETL_LOADER, ThrowingContinuousLoader.class.getName());
    SqoopOutputFormatLoadExecutor executor = new SqoopOutputFormatLoadExecutor(jobContextMock,
        ThrowingContinuousLoader.class.getName(), getIDF(), getMatcher());
  RecordWriter<SqoopWritable, NullWritable> writer = executor.getRecordWriter();
    IntermediateDataFormat<?> dataFormat = MRJobTestUtil.getTestIDF();
    SqoopWritable writable = new SqoopWritable(dataFormat);
    try {
      for (int i = 0; i < 10; i++) {
        StringBuilder builder = new StringBuilder();
        for (int count = 0; count < 100; count++) {
          builder.append(String.valueOf(count));
          if (count != 99) {
            builder.append(",");
          }
        }
        dataFormat.setCSVTextData(builder.toString());
        writer.write(writable, null);
      }
      writer.close(null);
    } catch (SqoopException ex) {
      throw ex.getCause();
    }
  }

}
