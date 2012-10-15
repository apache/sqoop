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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.CoreError;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.EtlContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.io.DataReader;
import org.apache.sqoop.utils.ClassLoadingUtils;

public class SqoopOutputFormatLoadExecutor {

  public static final Log LOG =
      LogFactory.getLog(SqoopOutputFormatLoadExecutor.class.getName());

  private boolean readerFinished;
  private boolean writerFinished;
  private Data data;
  private JobContext context;
  private SqoopRecordWriter producer;
  private ConsumerThread consumer;

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

  public class SqoopRecordWriter extends RecordWriter<Data, NullWritable> {
    @Override
    public void write(Data key, NullWritable value) {
      synchronized (data) {
        if (readerFinished) {
          consumer.checkException();
          return;
        }

        try {
          if (!data.isEmpty()) {
            // wait for reader to consume data
            data.wait();
          }

          int type = key.getType();
          data.setContent(key.getContent(type), type);

          // notify reader that the data is ready
          data.notify();

        } catch (InterruptedException e) {
          // inform reader that writer is finished
          writerFinished = true;

          // unlock reader so it can continue
          data.notify();

          // throw exception
          throw new SqoopException(CoreError.CORE_0015, e);
        }
      }
    }

    @Override
    public void close(TaskAttemptContext context) {
      synchronized (data) {
        if (readerFinished) {
          consumer.checkException();
          return;
        }

        try {
          if (!data.isEmpty()) {
            // wait for reader to consume data
            data.wait();
          }

          writerFinished = true;

          data.notify();

        } catch (InterruptedException e) {
          // inform reader that writer is finished
          writerFinished = true;

          // unlock reader so it can continue
          data.notify();

          // throw exception
          throw new SqoopException(CoreError.CORE_0015, e);
        }
      }
    }
  }

  public class OutputFormatDataReader extends DataReader {
    @Override
    public void setFieldDelimiter(char fieldDelimiter) {
      data.setFieldDelimiter(fieldDelimiter);
    }

    @Override
    public Object[] readArrayRecord() {
      return (Object[])readContent(Data.ARRAY_RECORD);
    }

    @Override
    public String readCsvRecord() {
      return (String)readContent(Data.CSV_RECORD);
    }

    @Override
    public Object readContent(int type) {
      synchronized (data) {
        if (writerFinished) {
          return null;
        }

        try {
          if (data.isEmpty()) {
            // wait for writer to produce data
            data.wait();
          }

          Object content = data.getContent(type);
          data.setContent(null, Data.EMPTY_DATA);

          // notify writer that data is consumed
          data.notify();

          return content;

        } catch (InterruptedException e) {
          // inform writer that reader is finished
          readerFinished = true;

          // unlock writer so it can continue
          data.notify();

          // throw exception
          throw new SqoopException(CoreError.CORE_0016, e);
        }
      }
    }
  }

  public class ConsumerThread extends Thread {
    private SqoopException exception = null;

    public void checkException() {
      if (exception != null) {
        throw exception;
      }
    }

    @Override
    public void run() {
      DataReader reader = new OutputFormatDataReader();

      Configuration conf = context.getConfiguration();

      try {
        String loaderName = conf.get(JobConstants.JOB_ETL_LOADER);
        Class<?> clz = ClassLoadingUtils.loadClass(loaderName);
        if (clz == null) {
          throw new SqoopException(CoreError.CORE_0009, loaderName);
        }

        Loader loader;
        try {
          loader = (Loader) clz.newInstance();
        } catch (Exception e) {
          throw new SqoopException(CoreError.CORE_0010, loaderName, e);
        }

        try {
          loader.run(new EtlContext(conf), reader);

        } catch (Throwable t) {
          throw new SqoopException(CoreError.CORE_0018, t);
        }

      } catch (SqoopException e) {
        exception = e;
      }

      synchronized (data) {
        // inform writer that reader is finished
        readerFinished = true;

        // unlock writer so it can continue
        data.notify();

        // if no exception happens yet
        if (exception == null && !writerFinished) {
          // create exception if data are not all consumed
          exception = new SqoopException(CoreError.CORE_0019);
        }

        // throw deferred exception if exist
        if (exception != null) {
          throw exception;
        }
      }
    }
  }

}
