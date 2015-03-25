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

import java.io.IOException;
import java.util.concurrent.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.submission.counter.SqoopCounters;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.utils.ClassUtils;

public class SqoopOutputFormatLoadExecutor {

  public static final Logger LOG =
    Logger.getLogger(SqoopOutputFormatLoadExecutor.class);

  private volatile boolean readerFinished = false;
  private volatile boolean writerFinished = false;
  private volatile IntermediateDataFormat<? extends Object> toDataFormat;
  private Matcher matcher;
  private JobContext context;
  private SqoopRecordWriter writer;
  private Future<?> consumerFuture;
  private Semaphore filled = new Semaphore(0, true);
  private Semaphore free = new Semaphore(1, true);
  private String loaderName;

  // NOTE: This method is only exposed for test cases
  SqoopOutputFormatLoadExecutor(JobContext jobctx, String loaderName, IntermediateDataFormat<?> toDataFormat, Matcher matcher) {
    context = jobctx;
    this.loaderName = loaderName;
    this.matcher = matcher;
    this.toDataFormat = toDataFormat;
    writer = new SqoopRecordWriter();
  }

  public SqoopOutputFormatLoadExecutor(JobContext jobctx) {
    context = jobctx;
    loaderName = context.getConfiguration().get(MRJobConstants.JOB_ETL_LOADER);
    writer = new SqoopRecordWriter();
    matcher = MatcherFactory.getMatcher(
        MRConfigurationUtils.getConnectorSchema(Direction.FROM, context.getConfiguration()),
        MRConfigurationUtils.getConnectorSchema(Direction.TO, context.getConfiguration()));
    toDataFormat = (IntermediateDataFormat<?>) ClassUtils.instantiate(context
        .getConfiguration().get(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT));
    // Using the TO schema since the SqoopDataWriter in the SqoopMapper encapsulates the toDataFormat
    toDataFormat.setSchema(matcher.getToSchema());
  }

  public RecordWriter<SqoopWritable, NullWritable> getRecordWriter() {
    consumerFuture = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat
        ("OutputFormatLoader-consumer").build()).submit(
            new ConsumerThread(context));
    return writer;
  }

  /*
   * This is a reader-writer problem and can be solved
   * with two semaphores.
   */
  private class SqoopRecordWriter extends RecordWriter<SqoopWritable, NullWritable> {

    @Override
    public void write(SqoopWritable key, NullWritable value) throws InterruptedException {
      free.acquire();
      checkIfConsumerThrew();
      // NOTE: this is the place where data written from SqoopMapper writable is available to the SqoopOutputFormat
      toDataFormat.setCSVTextData(key.toString());
      filled.release();
    }

    @Override
    public void close(TaskAttemptContext context)
            throws InterruptedException, IOException {
      LOG.info("SqoopOutputFormatLoadExecutor::SqoopRecordWriter is about to be closed");
      free.acquire();
      writerFinished = true;
      filled.release();
      waitForConsumer();
      LOG.info("SqoopOutputFormatLoadExecutor::SqoopRecordWriter is closed");
    }
  }

  private void checkIfConsumerThrew() {
    if(readerFinished) {
      waitForConsumer();
    }
  }
  /**
   * This method checks if the reader thread has finished, and re-throw
   * any exceptions thrown by the reader thread.
   *
   * @throws SqoopException if the consumer thread threw it.
   * @throws RuntimeException if some other exception was thrown.
   */
  private void waitForConsumer() {
    try {
      consumerFuture.get();
    } catch (ExecutionException ex) {
      // In almost all cases, the exception will be SqoopException,
      // because all exceptions are caught and propagated as
      // SqoopExceptions
      Throwable t = ex.getCause();
      if (t instanceof SqoopException) {
        throw (SqoopException) t;
      }
      //In the rare case, it was not a SqoopException
      Throwables.propagate(t);
    } catch (Exception ex) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0019, ex);
    }
  }

  private class SqoopOutputFormatDataReader extends DataReader {

    @Override
    public Object[] readArrayRecord() throws InterruptedException {
      acquireSema();
      // If the writer has finished, there is definitely no data remaining
      if (writerFinished) {
        return null;
      }
      try {
        return toDataFormat.getObjectData();
      } finally {
        releaseSema();
      }
    }

    @Override
    public String readTextRecord() throws InterruptedException {
      acquireSema();
      // If the writer has finished, there is definitely no data remaining
      if (writerFinished) {
        return null;
      }
      try {
        return toDataFormat.getCSVTextData();
      } finally {
        releaseSema();
      }
    }

    @Override
    public Object readContent() throws InterruptedException {
      acquireSema();
      if (writerFinished) {
        return null;
      }
      try {
        return toDataFormat.getData();
      } catch (Throwable t) {
        readerFinished = true;
        LOG.error("Caught exception e while getting content ", t);
        throw new SqoopException(MRExecutionError.MAPRED_EXEC_0018, t);
      } finally {
        releaseSema();
      }
    }

    private void acquireSema() throws InterruptedException {
      // Has any more data been produced after I last consumed.
      // If no, wait for the producer to produce.
      try {
        filled.acquire();
      } catch (InterruptedException ex) {
        //Really at this point, there is nothing to do. Just throw and get out
        LOG.error("Interrupted while waiting for data to be available from " +
          "mapper", ex);
        throw ex;
      }
    }

    private void releaseSema(){
      free.release();
    }
  }

  private class ConsumerThread implements Runnable {

    /**
     * Context class that we should use for reporting counters.
     */
    private final JobContext jobctx;

    public ConsumerThread(final JobContext context) {
      jobctx = context;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void run() {
      LOG.info("SqoopOutputFormatLoadExecutor consumer thread is starting");
      try {
        DataReader reader = new SqoopOutputFormatDataReader();
        Configuration conf = context.getConfiguration();
        Loader loader = (Loader) ClassUtils.instantiate(loaderName);

        // Objects that should be passed to the Loader
        PrefixContext subContext = new PrefixContext(conf,
            MRJobConstants.PREFIX_CONNECTOR_TO_CONTEXT);
        Object connectorLinkConfig = MRConfigurationUtils
            .getConnectorLinkConfig(Direction.TO, conf);
        Object connectorToJobConfig = MRConfigurationUtils
            .getConnectorJobConfig(Direction.TO, conf);
        // Using the TO schema since the SqoopDataWriter in the SqoopMapper
        // encapsulates the toDataFormat

        // Create loader context
        LoaderContext loaderContext = new LoaderContext(subContext, reader, matcher.getToSchema());

        LOG.info("Running loader class " + loaderName);
        loader.load(loaderContext, connectorLinkConfig, connectorToJobConfig);
        LOG.info("Loader has finished");
        ((TaskAttemptContext) jobctx).getCounter(SqoopCounters.ROWS_WRITTEN).increment(
            loader.getRowsWritten());

      } catch (Throwable t) {
        readerFinished = true;
        LOG.error("Error while loading data out of MR job.", t);
        // Release so that the writer can tell Sqoop something went
        // wrong.
        free.release();
        throw new SqoopException(MRExecutionError.MAPRED_EXEC_0018, t);
      }

      // if no exception happens yet and reader finished before writer,
      // something went wrong
      if (!writerFinished) {
        // throw exception if data are not all consumed
        readerFinished = true;
        LOG.error("Reader terminated, but writer is still running!");
        // Release so that the writer can tell Sqoop something went
        // wrong.
        free.release();
        throw new SqoopException(MRExecutionError.MAPRED_EXEC_0019);

      }
      // inform writer that reader is finished
      readerFinished = true;
    }
  }
}
