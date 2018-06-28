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
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.mapreduce.ParquetImportMapper;

import java.io.IOException;

/**
 * An implementation of {@link ParquetImportMapper} which depends on the Hadoop Parquet library.
 */
public class HadoopParquetImportMapper extends ParquetImportMapper<NullWritable, GenericRecord> {

  private static final Log LOG = LogFactory.getLog(HadoopParquetImportMapper.class.getName());

  /**
   * The key to get the configuration value set by
   * parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  private static final String HADOOP_PARQUET_AVRO_SCHEMA_KEY = "parquet.avro.schema";

  @Override
  protected LargeObjectLoader createLobLoader(Context context) throws IOException, InterruptedException {
    return new LargeObjectLoader(context.getConfiguration(), FileOutputFormat.getWorkOutputPath(context));
  }

  @Override
  protected Schema getAvroSchema(Configuration configuration) {
    String schemaString = configuration.get(HADOOP_PARQUET_AVRO_SCHEMA_KEY);
    LOG.debug("Found Avro schema: " + schemaString);
    return AvroUtil.parseAvroSchema(schemaString);
  }

  @Override
  protected void write(Context context, GenericRecord record) throws IOException, InterruptedException {
    context.write(null, record);
  }
}
