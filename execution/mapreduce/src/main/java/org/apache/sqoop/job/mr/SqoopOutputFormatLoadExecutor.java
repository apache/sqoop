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

import com.google.common.base.Throwables;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
  private Future<?> consumerFuture;
  private Semaphore filled = new Semaphore(0, true);
  private Semaphore free = new Semaphore(1, true);

  public SqoopOutputFormatLoadExecutor(JobContext jobctx) {
    data = new Data();
    context = jobctx;
    producer = new SqoopRecordWriter();
  }

  public RecordWriter<Data, NullWritable> getRecordWriter() {
    consumerFuture = Executors.newSingleThreadExecutor().submit(
            new ConsumerThread());
    return producer;
  }

  /*
   * This is a producer-consumer problem and can be solved
   * with two semaphores.
   */
  private class SqoopRecordWriter extends RecordWriter<Data, NullWritable> {

    @Override
    public void write(Data key, NullWritable value) throws InterruptedException {
      checkConsumerCompletion();
      free.acquire();
      int type = key.getType();
      data.setContent(key.getContent(type), type);
      filled.release();
    }

    @Override
    public void close(TaskAttemptContext context) throws InterruptedException {
      checkConsumerCompletion();
      free.acquire();
      writerFinished = true;
      // This will interrupt only the acquire call in the consumer class,
      // since we have acquired the free semaphore, and close is called from
      // the same thread that writes - so filled has not been released since then
      // so the consumer is definitely blocked on the filled semaphore.
      consumerFuture.cancel(true);
    }
  }

  /**
   * This method checks if the reader thread has finished, and re-throw
   * any exceptions thrown by the reader thread.
   *
   * @throws SqoopException if the consumer thread threw it.
   * @throws RuntimeException if some other exception was thrown.
   */
  private void checkConsumerCompletion() {
    if (readerFinished) {
      try {
        consumerFuture.get();
      } catch (ExecutionException ex) {
        // In almost all cases, the exception will be SqoopException,
        // because all exceptions are caught and propagated as
        // SqoopExceptions
        Throwable t = ex.getCause();
        if(t instanceof SqoopException) {
          throw (SqoopException)t;
        }
        //In the rare case, it was not a SqoopException
        Throwables.propagate(t);
      } catch (Exception ex) {
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0019, ex);
      }
    }
  }

  private class OutputFormatDataReader extends DataReader {
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

  private class ConsumerThread implements Runnable {

    @Override
    public void run() {
      DataReader reader = new OutputFormatDataReader();

      Configuration conf = context.getConfiguration();


      String loaderName = conf.get(JobConstants.JOB_ETL_LOADER);
      Loader loader = (Loader) ClassUtils.instantiate(loaderName);

      // Objects that should be pass to the Executor execution
      PrefixContext subContext = null;
      Object configConnection = null;
      Object configJob = null;

      switch (ConfigurationUtils.getJobType(conf)) {
        case EXPORT:
          subContext = new PrefixContext(conf, JobConstants.PREFIX_CONNECTOR_CONTEXT);
          configConnection = ConfigurationUtils.getConnectorConnection(conf);
          configJob = ConfigurationUtils.getConnectorJob(conf);
          break;
        case IMPORT:
          subContext = new PrefixContext(conf, "");
          configConnection = ConfigurationUtils.getFrameworkConnection(conf);
          configJob = ConfigurationUtils.getFrameworkJob(conf);
          break;
        default:
          throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0023);
      }

      try {
        loader.load(subContext, configConnection, configJob, reader);
      } catch (Throwable t) {
        LOG.error("Error while loading data out of MR job.", t);
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0018, t);
      }

      // if no exception happens yet and reader finished before writer,
      // something went wrong
      if (!writerFinished) {
        // throw exception if data are not all consumed
        LOG.error("Reader terminated, but writer is still running!");
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0019);

      }
      // inform writer that reader is finished
      readerFinished = true;
    }
  }
}
