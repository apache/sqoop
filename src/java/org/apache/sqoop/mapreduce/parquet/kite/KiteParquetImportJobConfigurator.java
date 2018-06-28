/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce.parquet.kite;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.mapreduce.parquet.ParquetImportJobConfigurator;
import org.apache.sqoop.util.FileSystemUtil;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

import java.io.IOException;

/**
 * An implementation of {@link ParquetImportJobConfigurator} which depends on the Kite Dataset API.
 */
public class KiteParquetImportJobConfigurator implements ParquetImportJobConfigurator {

  public static final Log LOG = LogFactory.getLog(KiteParquetImportJobConfigurator.class.getName());

  @Override
  public void configureMapper(Job job, Schema schema, SqoopOptions options, String tableName, Path destination) throws IOException {
    JobConf conf = (JobConf) job.getConfiguration();
    String uri = getKiteUri(conf, options, tableName, destination);
    KiteParquetUtils.WriteMode writeMode;

    if (options.doHiveImport()) {
      if (options.doOverwriteHiveTable()) {
        writeMode = KiteParquetUtils.WriteMode.OVERWRITE;
      } else {
        writeMode = KiteParquetUtils.WriteMode.APPEND;
        if (Datasets.exists(uri)) {
          LOG.warn("Target Hive table '" + tableName + "' exists! Sqoop will " +
              "append data into the existing Hive table. Consider using " +
              "--hive-overwrite, if you do NOT intend to do appending.");
        }
      }
    } else {
      // Note that there is no such an import argument for overwriting HDFS
      // dataset, so overwrite mode is not supported yet.
      // Sqoop's append mode means to merge two independent datasets. We
      // choose DEFAULT as write mode.
      writeMode = KiteParquetUtils.WriteMode.DEFAULT;
    }
    KiteParquetUtils.configureImportJob(conf, schema, uri, writeMode);
  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return KiteParquetImportMapper.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return DatasetKeyOutputFormat.class;
  }

  private String getKiteUri(Configuration conf, SqoopOptions options, String tableName, Path destination) throws IOException {
    if (options.doHiveImport()) {
      String hiveDatabase = options.getHiveDatabaseName() == null ? "default" :
          options.getHiveDatabaseName();
      String hiveTable = options.getHiveTableName() == null ? tableName :
          options.getHiveTableName();
      return String.format("dataset:hive:/%s/%s", hiveDatabase, hiveTable);
    } else {
      return "dataset:" + FileSystemUtil.makeQualified(destination, conf);
    }
  }
}
