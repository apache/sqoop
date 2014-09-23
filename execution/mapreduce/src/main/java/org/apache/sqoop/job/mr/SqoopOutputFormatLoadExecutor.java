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
import org.apache.sqoop.connector.idf.CSVIntermediateDataFormat;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.MapreduceExecutionError;
import org.apache.sqoop.common.PrefixContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.utils.ClassUtils;

public class SqoopOutputFormatLoadExecutor {

  public static final Logger LOG =
    Logger.getLogger(SqoopOutputFormatLoadExecutor.class);

  private volatile boolean readerFinished = false;
  private volatile boolean writerFinished = false;
  private volatile IntermediateDataFormat<String> dataFormat;
  private JobContext context;
  private SqoopRecordWriter writer;
  private Future<?> consumerFuture;
  private Semaphore filled = new Semaphore(0, true);
  private Semaphore free = new Semaphore(1, true);
  private volatile boolean isTest = false;
  private String loaderName;

  SqoopOutputFormatLoadExecutor(boolean isTest, String loaderName){
    this.isTest = isTest;
    this.loaderName = loaderName;
    dataFormat = new CSVIntermediateDataFormat();
    writer = new SqoopRecordWriter();
  }

  public SqoopOutputFormatLoadExecutor(JobContext jobctx) {
    context = jobctx;
    writer = new SqoopRecordWriter();
    dataFormat = (IntermediateDataFormat<String>) ClassUtils.instantiate(context
      .getConfiguration().get(JobConstants.INTERMEDIATE_DATA_FORMAT));

    Schema schema = ConfigurationUtils.getConnectorSchema(Direction.FROM, context.getConfiguration());
    if (schema==null) {
      schema = ConfigurationUtils.getConnectorSchema(Direction.TO, context.getConfiguration());
    }

    dataFormat.setSchema(schema);
  }

  public RecordWriter<SqoopWritable, NullWritable> getRecordWriter() {
    consumerFuture = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat
        ("OutputFormatLoader-consumer").build()).submit(
            new ConsumerThread());
    return writer;
  }

  /*
   * This is a producer-consumer problem and can be solved
   * with two semaphores.
   */
  private class SqoopRecordWriter extends RecordWriter<SqoopWritable, NullWritable> {

    @Override
    public void write(SqoopWritable key, NullWritable value) throws InterruptedException {
      free.acquire();
      checkIfConsumerThrew();
      dataFormat.setTextData(key.getString());
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
      throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0019, ex);
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
        return dataFormat.getObjectData();
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
        return dataFormat.getTextData();
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
        return dataFormat.getData();
      } catch (Throwable t) {
        readerFinished = true;
        LOG.error("Caught exception e while getting content ", t);
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0018, t);
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

    @Override
    public void run() {
      LOG.info("SqoopOutputFormatLoadExecutor consumer thread is starting");
      try {
        DataReader reader = new SqoopOutputFormatDataReader();

        Configuration conf = null;
        if (!isTest) {
          conf = context.getConfiguration();
          loaderName = conf.get(JobConstants.JOB_ETL_LOADER);
        }
        Loader loader = (Loader) ClassUtils.instantiate(loaderName);

        // Objects that should be pass to the Executor execution
        PrefixContext subContext = null;
        Object configConnection = null;
        Object configJob = null;
        Schema schema = null;

        if (!isTest) {
          // Propagate connector schema in every case for now
          // TODO: Change to coditional choosing between Connector schemas.
          // @TODO(Abe): Maybe use TO schema?
          schema = ConfigurationUtils.getConnectorSchema(Direction.FROM, conf);

          subContext = new PrefixContext(conf, JobConstants.PREFIX_CONNECTOR_TO_CONTEXT);
          configConnection = ConfigurationUtils.getConnectorConnectionConfig(Direction.TO, conf);
          configJob = ConfigurationUtils.getConnectorJobConfig(Direction.TO, conf);
        }

        // Create loader context
        LoaderContext loaderContext = new LoaderContext(subContext, reader, schema);

        LOG.info("Running loader class " + loaderName);
        loader.load(loaderContext, configConnection, configJob);
        LOG.info("Loader has finished");
      } catch (Throwable t) {
        readerFinished = true;
        LOG.error("Error while loading data out of MR job.", t);
        // Release so that the writer can tell Sqoop something went
        // wrong.
        free.release();
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0018, t);
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
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0019);

      }
      // inform writer that reader is finished
      readerFinished = true;
    }
  }
}
