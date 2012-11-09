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
import org.apache.sqoop.framework.SubmissionRequest;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.HdfsTextImportLoader;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.mr.SqoopFileOutputFormat;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopMapper;

/**
 *
 */
public class MapreduceExecutionEngine extends ExecutionEngine {

  @Override
  public SubmissionRequest createSubmissionRequest() {
    return new MRSubmissionRequest();
  }

  @Override
  public void prepareImportSubmission(SubmissionRequest gRequest) {
    MRSubmissionRequest request = (MRSubmissionRequest) gRequest;

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
    context.setString(JobConstants.JOB_ETL_LOADER, HdfsTextImportLoader.class.getName());
  }
}
