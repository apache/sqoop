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
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.submission.counter.SqoopCounters;
import org.apache.sqoop.utils.ClassUtils;

/**
 * A mapper to perform map function.
 */
public class SqoopMapper extends Mapper<SqoopSplit, NullWritable, SqoopWritable, NullWritable> {

  static {
    MRConfigurationUtils.configureLogging();
  }
  public static final Logger LOG = Logger.getLogger(SqoopMapper.class);

  /**
   * Service for reporting progress to mapreduce.
   */
  private final ScheduledExecutorService progressService = Executors.newSingleThreadScheduledExecutor();
  private IntermediateDataFormat<Object> fromIDF = null;
  private IntermediateDataFormat<Object> toIDF = null;
  private Matcher matcher;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String extractorName = conf.get(MRJobConstants.JOB_ETL_EXTRACTOR);
    Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

    Schema fromSchema = MRConfigurationUtils.getConnectorSchema(Direction.FROM, conf);
    Schema toSchema = MRConfigurationUtils.getConnectorSchema(Direction.TO, conf);
    matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

    String fromIDFClass = conf.get(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
    fromIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(fromIDFClass);
    fromIDF.setSchema(matcher.getFromSchema());
    String toIDFClass = conf.get(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
    toIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(toIDFClass);
    toIDF.setSchema(matcher.getToSchema());

    // Objects that should be passed to the Executor execution
    PrefixContext subContext = new PrefixContext(conf, MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
    Object fromConfig = MRConfigurationUtils.getConnectorLinkConfig(Direction.FROM, conf);
    Object fromJob = MRConfigurationUtils.getConnectorJobConfig(Direction.FROM, conf);

    SqoopSplit split = context.getCurrentKey();
    ExtractorContext extractorContext = new ExtractorContext(subContext, new SqoopMapDataWriter(context), fromSchema);

    try {
      LOG.info("Starting progress service");
      progressService.scheduleAtFixedRate(new SqoopProgressRunnable(context), 0, 2, TimeUnit.MINUTES);

      LOG.info("Running extractor class " + extractorName);
      extractor.extract(extractorContext, fromConfig, fromJob, split.getPartition());
      LOG.info("Extractor has finished");
      context.getCounter(SqoopCounters.ROWS_READ).increment(extractor.getRowsRead());
    } catch (Exception e) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0017, e);
    } finally {
      LOG.info("Stopping progress service");
      progressService.shutdown();
      if (!progressService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.info("Stopping progress service with shutdownNow");
        progressService.shutdownNow();
      }
    }
  }

  // There are two IDF objects we carry around in memory during the sqoop job execution.
  // The fromIDF has the fromSchema in it, the toIDF has the toSchema in it.
  // Before we do the writing to the toIDF object we do the matching process to negotiate between
  // the two schemas and their corresponding column types before we write the data to the toIDF object
  private class SqoopMapDataWriter extends DataWriter {
    private Context context;
    private SqoopWritable writable;

    public SqoopMapDataWriter(Context context) {
      this.context = context;
      this.writable = new SqoopWritable(toIDF);
    }

    @Override
    public void writeArrayRecord(Object[] array) {
      fromIDF.setObjectData(array);
      writeContent();
    }

    @Override
    public void writeStringRecord(String text) {
      fromIDF.setCSVTextData(text);
      writeContent();
    }

    @Override
    public void writeRecord(Object obj) {
      fromIDF.setData(obj);
      writeContent();
    }

    private void writeContent() {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Extracted data: " + fromIDF.getCSVTextData());
        }
        // NOTE: The fromIDF and the corresponding fromSchema is used only for the matching process
        // The output of the mappers is finally written to the toIDF object after the matching process
        // since the writable encapsulates the toIDF ==> new SqoopWritable(toIDF)
        toIDF.setObjectData(matcher.getMatchingData(fromIDF.getObjectData()));
        // NOTE: We do not use the reducer to do the writing (a.k.a LOAD in ETL). Hence the mapper sets up the writable
        context.write(writable, NullWritable.get());
      } catch (Exception e) {
        throw new SqoopException(MRExecutionError.MAPRED_EXEC_0013, e);
      }
    }
  }
}
