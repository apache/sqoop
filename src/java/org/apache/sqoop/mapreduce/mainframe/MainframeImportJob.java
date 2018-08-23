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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.mapreduce.DataDrivenImportJob;
import org.apache.sqoop.mapreduce.RawKeyTextOutputFormat;
import org.apache.sqoop.mapreduce.ByteKeyOutputFormat;
import org.apache.sqoop.mapreduce.parquet.ParquetImportJobConfigurator;

/**
 * Import data from a mainframe dataset, using MainframeDatasetInputFormat.
 */
public class MainframeImportJob extends DataDrivenImportJob {

  private static final Log LOG = LogFactory.getLog(
      MainframeImportJob.class.getName());

  public MainframeImportJob(final SqoopOptions opts, ImportJobContext context, ParquetImportJobConfigurator parquetImportJobConfigurator) {
    super(opts, MainframeDatasetInputFormat.class, context, parquetImportJobConfigurator);
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    if (SqoopOptions.FileLayout.BinaryFile.equals(options.getFileLayout())) {
      LOG.debug("Using MainframeDatasetBinaryImportMapper");
      return MainframeDatasetBinaryImportMapper.class;
    } else if (options.getFileLayout() == SqoopOptions.FileLayout.TextFile) {
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
    job.getConfiguration().set(
            MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,
            options.getMainframeInputDatasetType());
    job.getConfiguration().set(
            MainframeConfiguration.MAINFRAME_INPUT_DATASET_TAPE,
            options.getMainframeInputDatasetTape().toString());
    if (SqoopOptions.FileLayout.BinaryFile == options.getFileLayout()) {
      job.getConfiguration().set(
        MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE,
        MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE_BINARY);
      job.getConfiguration().setInt(
        MainframeConfiguration.MAINFRAME_FTP_TRANSFER_BINARY_BUFFER_SIZE,
        options.getBufferSize()
      );
    } else {
      job.getConfiguration().set(
        MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE,
        MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE_ASCII);
    }

  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    super.configureOutputFormat(job, tableName, tableClassName);
    job.getConfiguration().set(
      MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE,
      options.getMainframeFtpTransferMode());
    // use the default outputformat
    LazyOutputFormat.setOutputFormatClass(job, getOutputFormatClass());
  }

  @Override
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws IOException {
    super.configureMapper(job, tableName, tableClassName);
    if (SqoopOptions.FileLayout.BinaryFile == options.getFileLayout()) {
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(NullWritable.class);

      // this is required as code generated class assumes setField method takes String
      // and will fail with ClassCastException when a byte array is passed instead
      // java.lang.ClassCastException: [B cannot be cast to java.lang.String
      Configuration conf = job.getConfiguration();
      conf.setClass(org.apache.sqoop.mapreduce.db.DBConfiguration.INPUT_CLASS_PROPERTY, MainframeDatasetBinaryRecord.class,
        DBWritable.class);
    }
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    if (options.getFileLayout() == SqoopOptions.FileLayout.TextFile) {
      return RawKeyTextOutputFormat.class;
    } else if (options.getFileLayout()
        == SqoopOptions.FileLayout.BinaryFile) {
      return ByteKeyOutputFormat.class;
    }
    return null;
  }
}
