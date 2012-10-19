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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.CoreError;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.EtlContext;
import org.apache.sqoop.job.etl.EtlFramework;
import org.apache.sqoop.job.etl.EtlMutableContext;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.EtlOptions;
import org.apache.sqoop.job.etl.EtlOptions.JobType;
import org.apache.sqoop.job.io.Data;

/**
 * This class encapsulates the whole MapReduce execution.
 */
public class MrExecution {

  private Configuration conf;
  private EtlFramework etl;

  public MrExecution(EtlFramework etl) {
    this.conf = new Configuration();
    this.etl = etl;
  }

  public void initialize() {
    EtlOptions options = etl.getOptions();

    conf.setInt(JobConstants.JOB_ETL_NUMBER_PARTITIONS,
        options.getMaxExtractors());

    if (options.getOutputCodec() != null) {
      conf.setBoolean(FileOutputFormat.COMPRESS, true);
      conf.set(FileOutputFormat.COMPRESS_CODEC, options.getOutputCodec());
    }

    conf.set(JobConstants.JOB_ETL_PARTITIONER, etl.getPartitioner().getName());
    conf.set(JobConstants.JOB_ETL_EXTRACTOR, etl.getExtractor().getName());
    conf.set(JobConstants.JOB_ETL_LOADER, etl.getLoader().getName());

    EtlMutableContext context = new EtlMutableContext(conf);

    Class<? extends Initializer> initializer = etl.getInitializer();
    if (initializer != null) {
      Initializer instance;
      try {
        instance = (Initializer) initializer.newInstance();
      } catch (Exception e) {
        throw new SqoopException(CoreError.CORE_0010, initializer.getName(), e);
      }
      instance.run(context, options);
    }

    JobType jobType = etl.getOptions().getJobType();
    switch (jobType) {
    case IMPORT:
      checkImportConfiguration(context);
      break;
    case EXPORT:
      checkExportConfiguration(context);
      break;
    default:
      throw new SqoopException(CoreError.CORE_0012, jobType.toString());
    }
  }

  public void run() {
    EtlOptions options = etl.getOptions();

    try {
      Job job = Job.getInstance(conf);

      job.setInputFormatClass(SqoopInputFormat.class);
      job.setMapperClass(SqoopMapper.class);
      job.setMapOutputKeyClass(Data.class);
      job.setMapOutputValueClass(NullWritable.class);
      if (options.getMaxLoaders() > 1) {
        job.setReducerClass(SqoopReducer.class);
        job.setNumReduceTasks(options.getMaxLoaders());
      }
      job.setOutputFormatClass((etl.isOutputDirectoryRequired()) ?
              SqoopFileOutputFormat.class : SqoopNullOutputFormat.class);
      job.setOutputKeyClass(Data.class);
      job.setOutputValueClass(NullWritable.class);

      boolean success = job.waitForCompletion(true);
      if (!success) {
        throw new SqoopException(CoreError.CORE_0008);
      }

    } catch (Exception e) {
      throw new SqoopException(CoreError.CORE_0008, e);
    }
  }

  public void destroy() {
    Class<? extends Destroyer> destroyer = etl.getDestroyer();
    if (destroyer != null) {
      Destroyer instance;
      try {
        instance = (Destroyer) destroyer.newInstance();
      } catch (Exception e) {
        throw new SqoopException(CoreError.CORE_0010, destroyer.getName(), e);
      }
      instance.run(new EtlContext(conf));
    }
  }

  private void checkImportConfiguration(EtlMutableContext context) {
    if (etl.isFieldNamesRequired() &&
        context.getString(JobConstants.JOB_ETL_FIELD_NAMES) == null) {
      throw new SqoopException(CoreError.CORE_0020, "field names");
    }

    if (etl.isOutputDirectoryRequired()) {
      String outputDirectory =
          context.getString(JobConstants.JOB_ETL_OUTPUT_DIRECTORY);
      if (outputDirectory == null) {
        throw new SqoopException(CoreError.CORE_0020, "output directory");
      } else {
        context.setString(FileOutputFormat.OUTDIR, outputDirectory);
      }
    }
  }

  private void checkExportConfiguration(EtlMutableContext context) {
    // TODO: check export related configuration
  }

}
