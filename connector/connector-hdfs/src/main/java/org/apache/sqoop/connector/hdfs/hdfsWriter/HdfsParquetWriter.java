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
package org.apache.sqoop.connector.hdfs.hdfsWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.sqoop.connector.idf.AVROIntermediateDataFormat;
import org.apache.sqoop.schema.Schema;

import java.io.IOException;

public class HdfsParquetWriter extends GenericHdfsWriter {

  private ParquetWriter avroParquetWriter;
  private Schema sqoopSchema;
  private AVROIntermediateDataFormat avroIntermediateDataFormat;

  @Override
  public void initialize(Path filepath, Schema schema, Configuration conf, CompressionCodec hadoopCodec) throws IOException {
    sqoopSchema = schema;
    avroIntermediateDataFormat = new AVROIntermediateDataFormat(sqoopSchema);

    CompressionCodecName parquetCodecName;
    if (hadoopCodec == null) {
      parquetCodecName = CompressionCodecName.UNCOMPRESSED;
    } else {
      parquetCodecName = CompressionCodecName.fromCompressionCodec(hadoopCodec.getClass());
    }

    avroParquetWriter =
      AvroParquetWriter.builder(filepath)
        .withSchema(avroIntermediateDataFormat.getAvroSchema())
        .withCompressionCodec(parquetCodecName)
        .withConf(conf).build();

  }

  @Override
  public void write(Object[] record, String nullValue) throws IOException {
    avroParquetWriter.write(avroIntermediateDataFormat.toAVRO(record));
  }

  @Override
  public void destroy() throws IOException {
    avroParquetWriter.close();
  }
}
