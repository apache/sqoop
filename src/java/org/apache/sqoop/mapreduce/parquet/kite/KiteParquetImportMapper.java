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
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.mapreduce.ParquetImportMapper;

import java.io.IOException;

import static org.apache.sqoop.mapreduce.parquet.ParquetConstants.SQOOP_PARQUET_AVRO_SCHEMA_KEY;

public class KiteParquetImportMapper extends ParquetImportMapper<GenericRecord, Void> {

  @Override
  protected LargeObjectLoader createLobLoader(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Path workPath = new Path(conf.get("sqoop.kite.lob.extern.dir", "/tmp/sqoop-parquet-" + context.getTaskAttemptID()));
    return new LargeObjectLoader(conf, workPath);
  }

  @Override
  protected Schema getAvroSchema(Configuration configuration) {
    String schemaString = configuration.get(SQOOP_PARQUET_AVRO_SCHEMA_KEY);
    return AvroUtil.parseAvroSchema(schemaString);
  }

  @Override
  protected void write(Context context, GenericRecord record) throws IOException, InterruptedException {
    context.write(record, null);
  }
}
