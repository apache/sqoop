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
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.framework.ExecutionEngine;
import org.apache.sqoop.framework.SubmissionRequest;
import org.apache.sqoop.framework.configuration.ExportJobConfiguration;
import org.apache.sqoop.framework.configuration.ImportJobConfiguration;
import org.apache.sqoop.framework.configuration.OutputFormat;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.MapreduceExecutionError;
import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.HdfsExportExtractor;
import org.apache.sqoop.job.etl.HdfsExportPartitioner;
import org.apache.sqoop.job.etl.HdfsSequenceImportLoader;
import org.apache.sqoop.job.etl.HdfsTextImportLoader;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.mr.SqoopFileOutputFormat;
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

  /**
   *  {@inheritDoc}
   */
  @Override
  public void prepareImportSubmission(SubmissionRequest gRequest) {
    MRSubmissionRequest request = (MRSubmissionRequest) gRequest;
    ImportJobConfiguration jobConf = (ImportJobConfiguration) request.getConfigFrameworkJob();

    // Add jar dependencies
    addDependencies(request);

    // Configure map-reduce classes for import
    request.setInputFormatClass(SqoopInputFormat.class);

    request.setMapperClass(SqoopMapper.class);
    request.setMapOutputKeyClass(Data.class);
    request.setMapOutputValueClass(NullWritable.class);

    request.setOutputFormatClass(SqoopFileOutputFormat.class);
    request.setOutputKeyClass(Data.class);
    request.setOutputValueClass(NullWritable.class);

    Importer importer = (Importer)request.getConnectorCallbacks();

    // Set up framework context
    MutableMapContext context = request.getFrameworkContext();
    context.setString(JobConstants.JOB_ETL_PARTITIONER, importer.getPartitioner().getName());
    context.setString(JobConstants.JOB_ETL_EXTRACTOR, importer.getExtractor().getName());
    context.setString(JobConstants.JOB_ETL_DESTROYER, importer.getDestroyer().getName());

    if(request.getExtractors() != null) {
      context.setInteger(JobConstants.JOB_ETL_EXTRACTOR_NUM, request.getExtractors());
    }

    // TODO: This settings should be abstracted to core module at some point
    if(jobConf.output.outputFormat == OutputFormat.TEXT_FILE) {
      context.setString(JobConstants.JOB_ETL_LOADER, HdfsTextImportLoader.class.getName());
    } else if(jobConf.output.outputFormat == OutputFormat.SEQUENCE_FILE) {
      context.setString(JobConstants.JOB_ETL_LOADER, HdfsSequenceImportLoader.class.getName());
    } else {
      throw new SqoopException(MapreduceExecutionError.MAPRED_EXEC_0024,
        "Format: " + jobConf.output.outputFormat);
    }
  }

  /**
   *  {@inheritDoc}
   */
  @Override
  public void prepareExportSubmission(SubmissionRequest gRequest) {
    MRSubmissionRequest request = (MRSubmissionRequest) gRequest;
    ExportJobConfiguration jobConf = (ExportJobConfiguration) request.getConfigFrameworkJob();

    // Add jar dependencies
    addDependencies(request);

    // Configure map-reduce classes for import
    request.setInputFormatClass(SqoopInputFormat.class);

    request.setMapperClass(SqoopMapper.class);
    request.setMapOutputKeyClass(Data.class);
    request.setMapOutputValueClass(NullWritable.class);

    request.setOutputFormatClass(SqoopNullOutputFormat.class);
    request.setOutputKeyClass(Data.class);
    request.setOutputValueClass(NullWritable.class);

    Exporter exporter = (Exporter)request.getConnectorCallbacks();

    // Set up framework context
    MutableMapContext context = request.getFrameworkContext();
    context.setString(JobConstants.JOB_ETL_PARTITIONER, HdfsExportPartitioner.class.getName());
    context.setString(JobConstants.JOB_ETL_LOADER, exporter.getLoader().getName());
    context.setString(JobConstants.JOB_ETL_DESTROYER, exporter.getDestroyer().getName());

    // Extractor that will be able to read all supported file types
    context.setString(JobConstants.JOB_ETL_EXTRACTOR, HdfsExportExtractor.class.getName());
    context.setString(JobConstants.HADOOP_INPUTDIR, jobConf.input.inputDirectory);

    if(request.getExtractors() != null) {
      context.setInteger(JobConstants.JOB_ETL_EXTRACTOR_NUM, request.getExtractors());
    }
  }

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
