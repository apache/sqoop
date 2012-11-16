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

package org.apache.sqoop.job.mr;

import java.util.concurrent.Semaphore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.MapreduceExecutionError;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.io.DataReader;
import org.apache.sqoop.utils.ClassUtils;

public class SqoopOutputFormatLoadExecutor {

  public static final Log LOG =
      LogFactory.getLog(SqoopOutputFormatLoadExecutor.class.getName());

  private volatile boolean readerFinished = false;
  private volatile boolean writerFinished = false;
  private volatile Data data;
  private JobContext context;
  private SqoopRecordWriter producer;
  private ConsumerThread consumer;
  private Semaphore filled = new Semaphore(0, true);
  private Semaphore free = new Semaphore(1, true);

  public SqoopOutputFormatLoadExecutor(JobContext jobctx) {
    data = new Data();
    context = jobctx;
    producer = new SqoopRecordWriter();
    consumer = new ConsumerThread();
  }

  public RecordWriter<Data, NullWritable> getRecordWriter() {
    consumer.setDaemon(true);
    consumer.start();
    return producer;
  }

  /*
   * This is a producer-consumer problem and can be solved
   * with two semaphores.
   */
  public class SqoopRecordWriter extends RecordWriter<Data, NullWritable> {

    @Override
    public void write(Data key, NullWritable value) throws InterruptedException {

      if(readerFinished) {
        consumer.checkException();
      }
      free.acquire();
      int type = key.getType();
      data.setContent(key.getContent(type), type);
      filled.release();
    }

    @Override
    public void close(TaskAttemptContext context) throws InterruptedException {
      if(readerFinished) {
        // Reader finished before writer - something went wrong?
        consumer.checkException();
      }
      free.acquire();
      writerFinished = true;
      // This will interrupt only the acquire call in the consumer class,
      // since we have acquired the free semaphore, and close is called from
      // the same thread that writes - so filled has not been released since then
      // so the consumer is definitely blocked on the filled semaphore.
      consumer.interrupt();
    }
  }

  public class OutputFormatDataReader extends DataReader {
    @Override
    public void setFieldDelimiter(char fieldDelimiter) {
      data.setFieldDelimiter(fieldDelimiter);
    }

    @Override
    public Object[] readArrayRecord() throws InterruptedException {
      return (Object[])readContent(Data.ARRAY_RECORD);
    }

    @Override
    public String readCsvRecord() throws InterruptedException {
      return (String)readContent(Data.CSV_RECORD);
    }

    @Override
    public Object readContent(int type) throws InterruptedException {
      // Has any more data been produced after I last consumed.
      // If no, wait for the producer to produce.
      if (writerFinished && (filled.availablePermits() == 0)) {
        return null;
      }
      try {
        filled.acquire();
      } catch (InterruptedException ex) {
        if(writerFinished) {
          return null;
        }
        throw ex;
      }
      Object content = data.getContent(type);
      free.release();
      return content;
    }
  }

  public class ConsumerThread extends Thread {
    private volatile SqoopException exception = null;

    public void checkException() {
      if (exception != null) {
        throw exception;
      }
    }

    @Override
    public void run() {
      DataReader reader = new OutputFormatDataReader();

      Configuration conf = context.getConfiguration();


      String loaderName = conf.get(JobConstants.JOB_ETL_LOADER);
      Loader loader = (Loader) ClassUtils.instantiate(loaderName);

      // Get together framework context as configuration prefix by nothing
      PrefixContext frameworkContext = new PrefixContext(conf, "");

      try {
        loader.run(frameworkContext, reader);
      } catch (Throwable t) {
        exception = new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0018, t);
        LOG.error("Error while loading data out of MR job.", t);
      }

      // if no exception happens yet and reader finished before writer,
      // something went wrong
      if (exception == null && !writerFinished) {
        // create exception if data are not all consumed
        exception = new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0019);
        LOG.error("Reader terminated, but writer is still running!", exception);
      }
      // inform writer that reader is finished
      readerFinished = true;
    }
  }
}
