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

package org.apache.sqoop.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.spi.SchemaValidationUtil;

import java.io.IOException;

/**
 * Helper class for setting up a Parquet MapReduce job.
 */
public final class ParquetJob {

  public static final Log LOG = LogFactory.getLog(ParquetJob.class.getName());

  private ParquetJob() {
  }

  private static final String CONF_AVRO_SCHEMA = "parquetjob.avro.schema";
  static final String CONF_OUTPUT_CODEC = "parquetjob.output.codec";

  public static Schema getAvroSchema(Configuration conf) {
    return new Schema.Parser().parse(conf.get(CONF_AVRO_SCHEMA));
  }

  public static CompressionType getCompressionType(Configuration conf) {
    CompressionType defaults = Formats.PARQUET.getDefaultCompressionType();
    String codec = conf.get(CONF_OUTPUT_CODEC, defaults.getName());
    try {
      return CompressionType.forName(codec);
    } catch (IllegalArgumentException ex) {
      LOG.warn(String.format(
          "Unsupported compression type '%s'. Fallback to '%s'.",
          codec, defaults));
    }
    return defaults;
  }

  /**
   * Configure the import job. The import process will use a Kite dataset to
   * write data records into Parquet format internally. The input key class is
   * {@link org.apache.sqoop.lib.SqoopRecord}. The output key is
   * {@link org.apache.avro.generic.GenericRecord}.
   */
  public static void configureImportJob(Configuration conf, Schema schema,
      String uri, boolean reuseExistingDataset, boolean overwrite) throws IOException {
    Dataset dataset;
    if (reuseExistingDataset || overwrite) {
      try {
        dataset = Datasets.load(uri);
      } catch (DatasetNotFoundException ex) {
        dataset = createDataset(schema, getCompressionType(conf), uri);
      }
      Schema writtenWith = dataset.getDescriptor().getSchema();
      if (!SchemaValidationUtil.canRead(writtenWith, schema)) {
        throw new IOException(
            String.format("Expected schema: %s%nActual schema: %s",
                writtenWith, schema));
      }
    } else {
      dataset = createDataset(schema, getCompressionType(conf), uri);
    }
    conf.set(CONF_AVRO_SCHEMA, schema.toString());

    if (overwrite) {
      DatasetKeyOutputFormat.configure(conf).overwrite(dataset);
    } else {
      DatasetKeyOutputFormat.configure(conf).writeTo(dataset);
    }
  }

  private static Dataset createDataset(Schema schema,
      CompressionType compressionType, String uri) {
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .format(Formats.PARQUET)
        .compressionType(compressionType)
        .build();
    return Datasets.create(uri, descriptor, GenericRecord.class);
  }

}
