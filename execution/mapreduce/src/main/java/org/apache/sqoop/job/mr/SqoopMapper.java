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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.MapreduceExecutionError;
import org.apache.sqoop.common.PrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.submission.counter.SqoopCounters;
import org.apache.sqoop.utils.ClassUtils;

/**
 * A mapper to perform map function.
 */
public class SqoopMapper extends Mapper<SqoopSplit, NullWritable, SqoopWritable, NullWritable> {

  static {
    ConfigurationUtils.configureLogging();
  }
  public static final Logger LOG = Logger.getLogger(SqoopMapper.class);

  /**
   * Service for reporting progress to mapreduce.
   */
  private final ScheduledExecutorService progressService = Executors.newSingleThreadScheduledExecutor();
  private IntermediateDataFormat data = null;
  private SqoopWritable dataOut = null;

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String extractorName = conf.get(JobConstants.JOB_ETL_EXTRACTOR);
    Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

    // Propagate connector schema in every case for now
    // TODO: Change to coditional choosing between Connector schemas.

    Schema schema = ConfigurationUtils.getConnectorSchema(Direction.FROM, conf);
    if (schema==null) {
      schema = ConfigurationUtils.getConnectorSchema(Direction.TO, conf);
    }

    if (schema==null) {
      LOG.info("setting an empty schema");
    }


    String intermediateDataFormatName = conf.get(JobConstants
      .INTERMEDIATE_DATA_FORMAT);
    data = (IntermediateDataFormat) ClassUtils.instantiate(intermediateDataFormatName);
    data.setSchema(schema);
    dataOut = new SqoopWritable();

    // Objects that should be pass to the Executor execution
    PrefixContext subContext = null;
    Object configConnection = null;
    Object configJob = null;

    // Get configs for extractor
    subContext = new PrefixContext(conf, JobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
    configConnection = ConfigurationUtils.getConnectorConnectionConfig(Direction.FROM, conf);
    configJob = ConfigurationUtils.getConnectorJobConfig(Direction.FROM, conf);

    SqoopSplit split = context.getCurrentKey();
    ExtractorContext extractorContext = new ExtractorContext(subContext, new MapDataWriter(context), schema);

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

  private class MapDataWriter extends DataWriter {
    private Context context;

    public MapDataWriter(Context context) {
      this.context = context;
    }

    @Override
    public void writeArrayRecord(Object[] array) {
      data.setObjectData(array);
      writeContent();
    }

    @Override
    public void writeStringRecord(String text) {
      data.setTextData(text);
      writeContent();
    }

    @Override
    public void writeRecord(Object obj) {
      data.setData(obj.toString());
      writeContent();
    }

    private void writeContent() {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Extracted data: " + data.getTextData());
        }
        dataOut.setString(data.getTextData());
        context.write(dataOut, NullWritable.get());
      } catch (Exception e) {
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0013, e);
      }
    }
  }
}
