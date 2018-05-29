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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.mapreduce.MergeParquetMapper;
import org.apache.sqoop.mapreduce.parquet.ParquetMergeJobConfigurator;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.schema.MessageType;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.sqoop.mapreduce.parquet.ParquetConstants.SQOOP_PARQUET_AVRO_SCHEMA_KEY;

public class KiteParquetMergeJobConfigurator implements ParquetMergeJobConfigurator {

  public static final Log LOG = LogFactory.getLog(KiteParquetMergeJobConfigurator.class.getName());

  @Override
  public void configureParquetMergeJob(Configuration conf, Job job, Path oldPath, Path newPath,
                                       Path finalPath) throws IOException {
    try {
      FileSystem fileSystem = finalPath.getFileSystem(conf);
      LOG.info("Trying to merge parquet files");
      job.setOutputKeyClass(GenericRecord.class);
      job.setMapperClass(MergeParquetMapper.class);
      job.setReducerClass(KiteMergeParquetReducer.class);
      job.setOutputValueClass(NullWritable.class);

      List<Footer> footers = new ArrayList<Footer>();
      FileStatus oldPathfileStatus = fileSystem.getFileStatus(oldPath);
      FileStatus newPathfileStatus = fileSystem.getFileStatus(oldPath);
      footers.addAll(ParquetFileReader.readFooters(job.getConfiguration(), oldPathfileStatus, true));
      footers.addAll(ParquetFileReader.readFooters(job.getConfiguration(), newPathfileStatus, true));

      MessageType schema = footers.get(0).getParquetMetadata().getFileMetaData().getSchema();
      AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
      Schema avroSchema = avroSchemaConverter.convert(schema);

      if (!fileSystem.exists(finalPath)) {
        Dataset dataset = createDataset(avroSchema, "dataset:" + finalPath);
        DatasetKeyOutputFormat.configure(job).overwrite(dataset);
      } else {
        DatasetKeyOutputFormat.configure(job).overwrite(new URI("dataset:" + finalPath));
      }

      job.setInputFormatClass(AvroParquetInputFormat.class);
      AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);

      conf.set(SQOOP_PARQUET_AVRO_SCHEMA_KEY, avroSchema.toString());
      Class<DatasetKeyOutputFormat> outClass = DatasetKeyOutputFormat.class;

      job.setOutputFormatClass(outClass);
    } catch (Exception cnfe) {
      throw new IOException(cnfe);
    }
  }

  public static Dataset createDataset(Schema schema, String uri) {
    DatasetDescriptor descriptor =
        new DatasetDescriptor.Builder().schema(schema).format(Formats.PARQUET).build();
    return Datasets.create(uri, descriptor, GenericRecord.class);
  }
}
