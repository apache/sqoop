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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.job.io.Data;
import org.junit.Before;
import org.junit.Test;

import java.util.ConcurrentModificationException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

public class TestSqoopOutputFormatLoadExecutor {

  private Configuration conf;

  public static class ThrowingLoader extends Loader {

    public ThrowingLoader() {

    }

    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      context.getDataReader().readContent(Data.CSV_RECORD);
      throw new BrokenBarrierException();
    }
  }

  public static class ThrowingContinuousLoader extends Loader {

    public ThrowingContinuousLoader() {
    }

    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      int runCount = 0;
      Object o;
      String[] arr;
      while ((o = context.getDataReader().readContent(Data.CSV_RECORD)) != null) {
        arr = o.toString().split(",");
        Assert.assertEquals(100, arr.length);
        for (int i = 0; i < arr.length; i++) {
          Assert.assertEquals(i, Integer.parseInt(arr[i]));
        }
        runCount++;
        if (runCount == 5) {
          throw new ConcurrentModificationException();
        }
      }
    }
  }

  public static class GoodLoader extends Loader {

    public GoodLoader() {

    }

    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      String[] arr = context.getDataReader().readContent(Data.CSV_RECORD).toString().split(",");
      Assert.assertEquals(100, arr.length);
      for (int i = 0; i < arr.length; i++) {
        Assert.assertEquals(i, Integer.parseInt(arr[i]));
      }
    }
  }

  public static class GoodContinuousLoader extends Loader {

    public GoodContinuousLoader() {

    }

    @Override
    public void load(LoaderContext context, Object cc, Object jc) throws Exception {
      int runCount = 0;
      Object o;
      String[] arr;
      while ((o = context.getDataReader().readContent(Data.CSV_RECORD)) != null) {
        arr = o.toString().split(",");
        Assert.assertEquals(100, arr.length);
        for (int i = 0; i < arr.length; i++) {
          Assert.assertEquals(i, Integer.parseInt(arr[i]));
        }
        runCount++;
      }
      Assert.assertEquals(10, runCount);
    }
  }


  @Before
  public void setUp() {
    conf = new Configuration();

  }

  @Test(expected = BrokenBarrierException.class)
  public void testWhenLoaderThrows() throws Throwable {
    conf.set(JobConstants.JOB_TYPE, "EXPORT");
    conf.set(JobConstants.JOB_ETL_LOADER, ThrowingLoader.class.getName());
    SqoopOutputFormatLoadExecutor executor = new
        SqoopOutputFormatLoadExecutor(true, ThrowingLoader.class.getName());
    RecordWriter<Data, NullWritable> writer = executor.getRecordWriter();
    Data data = new Data();
    try {
      for (int count = 0; count < 100; count++) {
        data.setContent(String.valueOf(count), Data.CSV_RECORD);
        writer.write(data, null);
      }
    } catch (SqoopException ex) {
      throw ex.getCause();
    }
  }

  @Test
  public void testSuccessfulContinuousLoader() throws Throwable {
    conf.set(JobConstants.JOB_TYPE, "EXPORT");
    conf.set(JobConstants.JOB_ETL_LOADER, GoodContinuousLoader.class.getName());
    SqoopOutputFormatLoadExecutor executor = new
        SqoopOutputFormatLoadExecutor(true, GoodContinuousLoader.class.getName());
    RecordWriter<Data, NullWritable> writer = executor.getRecordWriter();
    Data data = new Data();
    for (int i = 0; i < 10; i++) {
      StringBuilder builder = new StringBuilder();
      for (int count = 0; count < 100; count++) {
        builder.append(String.valueOf(count));
        if (count != 99) {
          builder.append(",");
        }
      }
      data.setContent(builder.toString(), Data.CSV_RECORD);
      writer.write(data, null);
    }
    writer.close(null);
  }

  @Test (expected = SqoopException.class)
  public void testSuccessfulLoader() throws Throwable {
    SqoopOutputFormatLoadExecutor executor = new
        SqoopOutputFormatLoadExecutor(true, GoodLoader.class.getName());
    RecordWriter<Data, NullWritable> writer = executor.getRecordWriter();
    Data data = new Data();
    StringBuilder builder = new StringBuilder();
    for (int count = 0; count < 100; count++) {
      builder.append(String.valueOf(count));
      if (count != 99) {
        builder.append(",");
      }
    }
    data.setContent(builder.toString(), Data.CSV_RECORD);
    writer.write(data, null);
    //Allow writer to complete.
    TimeUnit.SECONDS.sleep(5);
    writer.close(null);
  }


  @Test(expected = ConcurrentModificationException.class)
  public void testThrowingContinuousLoader() throws Throwable {
    conf.set(JobConstants.JOB_TYPE, "EXPORT");
    conf.set(JobConstants.JOB_ETL_LOADER, ThrowingContinuousLoader.class.getName());
    SqoopOutputFormatLoadExecutor executor = new
        SqoopOutputFormatLoadExecutor(true, ThrowingContinuousLoader.class.getName());
    RecordWriter<Data, NullWritable> writer = executor.getRecordWriter();
    Data data = new Data();
    try {
      for (int i = 0; i < 10; i++) {
        StringBuilder builder = new StringBuilder();
        for (int count = 0; count < 100; count++) {
          builder.append(String.valueOf(count));
          if (count != 99) {
            builder.append(",");
          }
        }
        data.setContent(builder.toString(), Data.CSV_RECORD);
        writer.write(data, null);
      }
      writer.close(null);
    } catch (SqoopException ex) {
      throw ex.getCause();
    }
  }
}
