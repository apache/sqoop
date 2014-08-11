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
package org.apache.sqoop.execution.mapreduce;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.io.NullWritable;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.framework.ExecutionEngine;
import org.apache.sqoop.framework.SubmissionRequest;
import org.apache.sqoop.framework.configuration.JobConfiguration;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopMapper;
import org.apache.sqoop.job.mr.SqoopNullOutputFormat;

/**
 *
 */
public class MapreduceExecutionEngine extends ExecutionEngine {

  /**
   *  {@inheritDoc}
   */
  @Override
  public SubmissionRequest createSubmissionRequest() {
    return new MRSubmissionRequest();
  }

  public void prepareSubmission(SubmissionRequest gRequest) {
    MRSubmissionRequest request = (MRSubmissionRequest)gRequest;

    // Add jar dependencies
    addDependencies(request);

    // Configure map-reduce classes for import
    request.setInputFormatClass(SqoopInputFormat.class);

    request.setMapperClass(SqoopMapper.class);
    request.setMapOutputKeyClass(SqoopWritable.class);
    request.setMapOutputValueClass(NullWritable.class);

    request.setOutputFormatClass(SqoopNullOutputFormat.class);
    request.setOutputKeyClass(SqoopWritable.class);
    request.setOutputValueClass(NullWritable.class);

    // Set up framework context
    From from = (From)request.getFromCallback();
    To to = (To)request.getToCallback();
    MutableMapContext context = request.getFrameworkContext();
    context.setString(JobConstants.JOB_ETL_PARTITIONER, from.getPartitioner().getName());
    context.setString(JobConstants.JOB_ETL_EXTRACTOR, from.getExtractor().getName());
    context.setString(JobConstants.JOB_ETL_LOADER, to.getLoader().getName());
    context.setString(JobConstants.JOB_ETL_DESTROYER, from.getDestroyer().getName());
    context.setString(JobConstants.INTERMEDIATE_DATA_FORMAT,
        request.getIntermediateDataFormat().getName());

    if(request.getExtractors() != null) {
      context.setInteger(JobConstants.JOB_ETL_EXTRACTOR_NUM, request.getExtractors());
    }

    if(request.getExtractors() != null) {
      context.setInteger(JobConstants.JOB_ETL_EXTRACTOR_NUM, request.getExtractors());
    }

    // @TODO(Abe): Move to HDFS connector.
//    if(jobConf.output.outputFormat == OutputFormat.TEXT_FILE) {
//      context.setString(JobConstants.JOB_ETL_LOADER, HdfsTextImportLoader.class.getName());
//    } else if(jobConf.output.outputFormat == OutputFormat.SEQUENCE_FILE) {
//      context.setString(JobConstants.JOB_ETL_LOADER, HdfsSequenceImportLoader.class.getName());
//    } else {
//      throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0024,
//        "Format: " + jobConf.output.outputFormat);
//    }
//    if(getCompressionCodecName(jobConf) != null) {
//      context.setString(JobConstants.HADOOP_COMPRESS_CODEC,
//        getCompressionCodecName(jobConf));
//      context.setBoolean(JobConstants.HADOOP_COMPRESS, true);
//    }
  }

  // @TODO(Abe): Move to HDFS connector.
//  private String getCompressionCodecName(ImportJobConfiguration jobConf) {
//    if(jobConf.output.compression == null)
//      return null;
//    switch(jobConf.output.compression) {
//      case NONE:
//        return null;
//      case DEFAULT:
//        return "org.apache.hadoop.io.compress.DefaultCodec";
//      case DEFLATE:
//        return "org.apache.hadoop.io.compress.DeflateCodec";
//      case GZIP:
//        return "org.apache.hadoop.io.compress.GzipCodec";
//      case BZIP2:
//        return "org.apache.hadoop.io.compress.BZip2Codec";
//      case LZO:
//        return "com.hadoop.compression.lzo.LzoCodec";
//      case LZ4:
//        return "org.apache.hadoop.io.compress.Lz4Codec";
//      case SNAPPY:
//        return "org.apache.hadoop.io.compress.SnappyCodec";
//      case CUSTOM:
//        return jobConf.output.customCompression.trim();
//    }
//    return null;
//  }

  /**
   * Our execution engine have additional dependencies that needs to be available
   * at mapreduce job time. This method will register all dependencies in the request
   * object.
   *
   * @param request Active request object.
   */
  protected void addDependencies(MRSubmissionRequest request) {
    // Guava
    request.addJarForClass(ThreadFactoryBuilder.class);
  }
}
