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

import org.apache.hadoop.io.NullWritable;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.framework.ExecutionEngine;
import org.apache.sqoop.framework.JobRequest;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopMapper;
import org.apache.sqoop.job.mr.SqoopNullOutputFormat;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 *
 */
public class MapreduceExecutionEngine extends ExecutionEngine {

  /**
   *  {@inheritDoc}
   */
  @Override
  public JobRequest createJobRequest() {
    return new MRJobRequest();
  }

  public void prepareJob(JobRequest jobRequest) {
    MRJobRequest mrJobRequest = (MRJobRequest)jobRequest;

    // Add jar dependencies
    addDependencies(mrJobRequest);

    // Configure map-reduce classes for import
    mrJobRequest.setInputFormatClass(SqoopInputFormat.class);

    mrJobRequest.setMapperClass(SqoopMapper.class);
    mrJobRequest.setMapOutputKeyClass(SqoopWritable.class);
    mrJobRequest.setMapOutputValueClass(NullWritable.class);

    mrJobRequest.setOutputFormatClass(SqoopNullOutputFormat.class);
    mrJobRequest.setOutputKeyClass(SqoopWritable.class);
    mrJobRequest.setOutputValueClass(NullWritable.class);

    From from = (From) mrJobRequest.getFrom();
    To to = (To) mrJobRequest.getTo();

    MutableMapContext context = mrJobRequest.getFrameworkContext();
    context.setString(JobConstants.JOB_ETL_PARTITIONER, from.getPartitioner().getName());
    context.setString(JobConstants.JOB_ETL_EXTRACTOR, from.getExtractor().getName());
    context.setString(JobConstants.JOB_ETL_LOADER, to.getLoader().getName());
    context.setString(JobConstants.JOB_ETL_DESTROYER, from.getDestroyer().getName());
    context.setString(JobConstants.INTERMEDIATE_DATA_FORMAT,
        mrJobRequest.getIntermediateDataFormat().getName());

    if(mrJobRequest.getExtractors() != null) {
      context.setInteger(JobConstants.JOB_ETL_EXTRACTOR_NUM, mrJobRequest.getExtractors());
    }
  }



  /**
   * Our execution engine have additional dependencies that needs to be available
   * at mapreduce job time. This method will register all dependencies in the request
   * object.
   *
   * @param request Active request object.
   */
  protected void addDependencies(MRJobRequest request) {
    // Guava
    request.addJarForClass(ThreadFactoryBuilder.class);
  }
}
