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

package org.apache.sqoop.mapreduce.parquet.hadoop;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.parquet.avro.GenericDataSupplier;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.mapreduce.parquet.ParquetImportJobConfigurator;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

import static org.apache.sqoop.mapreduce.parquet.ParquetConstants.SQOOP_PARQUET_OUTPUT_CODEC_KEY;

/**
 * An implementation of {@link ParquetImportJobConfigurator} which depends on the Hadoop Parquet library.
 */
public class HadoopParquetImportJobConfigurator implements ParquetImportJobConfigurator {

  private static final Log LOG = LogFactory.getLog(HadoopParquetImportJobConfigurator.class.getName());

  @Override
  public void configureMapper(Job job, Schema schema, SqoopOptions options, String tableName, Path destination) throws IOException {
    configureAvroSchema(job, schema);
    configureOutputCodec(job);
    configureLogicalTypeSupport(job, options);
  }

  /**
   * Configurations needed for logical types, i.e. decimal in parquet.
   * @param job
   * @param options
   */
  private void configureLogicalTypeSupport(Job job, SqoopOptions options) {
    if (options.getConf().getBoolean(ConfigurationConstants.PROP_ENABLE_PARQUET_LOGICAL_TYPE_DECIMAL, false)) {
      AvroParquetOutputFormat.setAvroDataSupplier(job, GenericDataSupplier.class);
    }
  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return HadoopParquetImportMapper.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return AvroParquetOutputFormat.class;
  }

  @Override
  public boolean isHiveImportNeeded() {
    return true;
  }

  void configureOutputCodec(Job job) {
    String outputCodec = job.getConfiguration().get(SQOOP_PARQUET_OUTPUT_CODEC_KEY);
    if (outputCodec != null) {
      LOG.info("Using output codec: " + outputCodec);
      ParquetOutputFormat.setCompression(job, CompressionCodecName.fromConf(outputCodec));
    }
  }

  void configureAvroSchema(Job job, Schema schema) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using Avro schema: " + schema);
    }
    AvroParquetOutputFormat.setSchema(job, schema);
  }
}
