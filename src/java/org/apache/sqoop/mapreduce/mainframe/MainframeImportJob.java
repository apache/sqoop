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

package org.apache.sqoop.mapreduce.mainframe;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ImportJobContext;

import org.apache.sqoop.mapreduce.DataDrivenImportJob;

/**
 * Import data from a mainframe dataset, using MainframeDatasetInputFormat.
 */
public class MainframeImportJob extends DataDrivenImportJob {

  private static final Log LOG = LogFactory.getLog(
      MainframeImportJob.class.getName());

  public MainframeImportJob(final SqoopOptions opts, ImportJobContext context) {
    super(opts, MainframeDatasetInputFormat.class, context);
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    if (options.getFileLayout() == SqoopOptions.FileLayout.TextFile) {
      return MainframeDatasetImportMapper.class;
    } else {
      return super.getMapperClass();
    }
  }

  @Override
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol) throws IOException {
    super.configureInputFormat(job, tableName, tableClassName, splitByCol);
    job.getConfiguration().set(
        MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME,
        options.getMainframeInputDatasetName());
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    super.configureOutputFormat(job, tableName, tableClassName);
    LazyOutputFormat.setOutputFormatClass(job, getOutputFormatClass());
  }

}
