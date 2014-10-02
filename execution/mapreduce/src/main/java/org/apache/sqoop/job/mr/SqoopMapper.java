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
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.MapreduceExecutionError;
import org.apache.sqoop.common.PrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.etl.io.DataWriter;
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
  private IntermediateDataFormat<String> fromDataFormat = null;
  private IntermediateDataFormat<String> toDataFormat = null;
  private Matcher matcher;

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String extractorName = conf.get(JobConstants.JOB_ETL_EXTRACTOR);
    Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

    matcher = MatcherFactory.getMatcher(
        ConfigurationUtils.getConnectorSchema(Direction.FROM, conf),
        ConfigurationUtils.getConnectorSchema(Direction.TO, conf));

    String intermediateDataFormatName = conf.get(JobConstants.INTERMEDIATE_DATA_FORMAT);
    fromDataFormat = (IntermediateDataFormat<String>) ClassUtils
        .instantiate(intermediateDataFormatName);
    fromDataFormat.setSchema(matcher.getFromSchema());
    toDataFormat = (IntermediateDataFormat<String>) ClassUtils
        .instantiate(intermediateDataFormatName);
    toDataFormat.setSchema(matcher.getToSchema());

    // Objects that should be passed to the Executor execution
    PrefixContext subContext = new PrefixContext(conf, JobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
    Object fromConfig = ConfigurationUtils.getConnectorConnectionConfig(Direction.FROM, conf);
    Object fromJob = ConfigurationUtils.getConnectorJobConfig(Direction.FROM, conf);

    SqoopSplit split = context.getCurrentKey();
    ExtractorContext extractorContext = new ExtractorContext(subContext, new SqoopMapDataWriter(context));

    try {
      LOG.info("Starting progress service");
      progressService.scheduleAtFixedRate(new ProgressRunnable(context), 0, 2, TimeUnit.MINUTES);

      LOG.info("Running extractor class " + extractorName);
      extractor.extract(extractorContext, fromConfig, fromJob, split.getPartition());
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

  private class SqoopMapDataWriter extends DataWriter {
    private Context context;
    private SqoopWritable writable;

    public SqoopMapDataWriter(Context context) {
      this.context = context;
      this.writable = new SqoopWritable();
    }

    @Override
    public void writeArrayRecord(Object[] array) {
      fromDataFormat.setObjectData(array);
      writeContent();
    }

    @Override
    public void writeStringRecord(String text) {
      fromDataFormat.setTextData(text);
      writeContent();
    }

    @Override
    public void writeRecord(Object obj) {
      fromDataFormat.setData(obj.toString());
      writeContent();
    }

    private void writeContent() {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Extracted data: " + fromDataFormat.getTextData());
        }

        toDataFormat.setObjectData( matcher.getMatchingData( fromDataFormat.getObjectData() ) );

        writable.setString(toDataFormat.getTextData());
        context.write(writable, NullWritable.get());
      } catch (Exception e) {
        throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0013, e);
      }
    }
  }
}
