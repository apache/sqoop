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

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.MapreduceExecutionError;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.submission.counter.SqoopCounters;
import org.apache.sqoop.utils.ClassUtils;

/**
 * A mapper to perform map function.
 */
public class SqoopMapper extends Mapper<SqoopSplit, NullWritable, Data, NullWritable> {

  public static final Log LOG = LogFactory.getLog(SqoopMapper.class);

  /**
   * Service for reporting progress to mapreduce.
   */
  private final ScheduledExecutorService progressService = Executors.newSingleThreadScheduledExecutor();

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String extractorName = conf.get(JobConstants.JOB_ETL_EXTRACTOR);
    Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

    // Objects that should be pass to the Executor execution
    PrefixContext subContext = null;
    Object configConnection = null;
    Object configJob = null;

    // Executor is in connector space for IMPORT and in framework space for EXPORT
    switch (ConfigurationUtils.getJobType(conf)) {
      case IMPORT:
        subContext = new PrefixContext(conf, JobConstants.PREFIX_CONNECTOR_CONTEXT);
        configConnection = ConfigurationUtils.getConnectorConnection(conf);
        configJob = ConfigurationUtils.getConnectorJob(conf);
        break;
      case EXPORT:
        subContext = new PrefixContext(conf, "");
        configConnection = ConfigurationUtils.getFrameworkConnection(conf);
        configJob = ConfigurationUtils.getFrameworkJob(conf);
        break;
      default:
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0023);
    }

    SqoopSplit split = context.getCurrentKey();
    ExtractorContext extractorContext = new ExtractorContext(subContext, new MapDataWriter(context));

    try {
      LOG.info("Starting progress service");
      progressService.scheduleAtFixedRate(new ProgressRunnable(context), 0, 2, TimeUnit.MINUTES);

      LOG.info("Running extractor class " + extractorName);
      extractor.extract(extractorContext, configConnection, configJob, split.getPartition());
      LOG.info("Extractor has finished");
      context.getCounter(SqoopCounters.ROWS_READ)
              .increment(extractor.getRowsRead());
    } catch (Exception e) {
      throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0017, e);
    } finally {
      LOG.info("Stopping progress service");
      progressService.shutdown();
      if(!progressService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.info("Stopping progress service with shutdownNow");
        progressService.shutdownNow();
      }
    }
  }

  public class MapDataWriter extends DataWriter {
    private Context context;
    private Data data;

    public MapDataWriter(Context context) {
      this.context = context;
    }

    @Override
    public void setFieldDelimiter(char fieldDelimiter) {
      if (data == null) {
        data = new Data();
      }

      data.setFieldDelimiter(fieldDelimiter);
    }

    @Override
    public void writeArrayRecord(Object[] array) {
      writeContent(array, Data.ARRAY_RECORD);
    }

    @Override
    public void writeCsvRecord(String csv) {
      writeContent(csv, Data.CSV_RECORD);
    }

    @Override
    public void writeContent(Object content, int type) {
      if (data == null) {
        data = new Data();
      }

      data.setContent(content, type);
      try {
        context.write(data, NullWritable.get());
      } catch (Exception e) {
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0013, e);
      }
    }
  }

}
