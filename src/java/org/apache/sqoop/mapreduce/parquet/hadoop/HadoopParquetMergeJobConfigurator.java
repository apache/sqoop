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
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.mapreduce.MergeParquetMapper;
import org.apache.sqoop.mapreduce.parquet.ParquetMergeJobConfigurator;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.apache.sqoop.avro.AvroUtil.getAvroSchemaFromParquetFile;
import static org.apache.sqoop.mapreduce.parquet.ParquetConstants.SQOOP_PARQUET_AVRO_SCHEMA_KEY;

/**
 * An implementation of {@link ParquetMergeJobConfigurator} which depends on the Hadoop Parquet library.
 */
public class HadoopParquetMergeJobConfigurator implements ParquetMergeJobConfigurator {

  public static final Log LOG = LogFactory.getLog(HadoopParquetMergeJobConfigurator.class.getName());

  private final HadoopParquetImportJobConfigurator importJobConfigurator;

  private final HadoopParquetExportJobConfigurator exportJobConfigurator;

  public HadoopParquetMergeJobConfigurator(HadoopParquetImportJobConfigurator importJobConfigurator, HadoopParquetExportJobConfigurator exportJobConfigurator) {
    this.importJobConfigurator = importJobConfigurator;
    this.exportJobConfigurator = exportJobConfigurator;
  }

  public HadoopParquetMergeJobConfigurator() {
    this(new HadoopParquetImportJobConfigurator(), new HadoopParquetExportJobConfigurator());
  }

  @Override
  public void configureParquetMergeJob(Configuration conf, Job job, Path oldPath, Path newPath,
                                       Path finalPath) throws IOException {
    try {
      LOG.info("Trying to merge parquet files");
      job.setOutputKeyClass(Void.class);
      job.setMapperClass(MergeParquetMapper.class);
      job.setReducerClass(HadoopMergeParquetReducer.class);
      job.setOutputValueClass(GenericRecord.class);

      Schema avroSchema = loadAvroSchema(conf, oldPath);

      validateNewPathAvroSchema(getAvroSchemaFromParquetFile(newPath, conf), avroSchema);

      job.setInputFormatClass(exportJobConfigurator.getInputFormatClass());
      AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);

      conf.set(SQOOP_PARQUET_AVRO_SCHEMA_KEY, avroSchema.toString());
      importJobConfigurator.configureAvroSchema(job, avroSchema);
      importJobConfigurator.configureOutputCodec(job);
      job.setOutputFormatClass(importJobConfigurator.getOutputFormatClass());
    } catch (Exception cnfe) {
      throw new IOException(cnfe);
    }
  }

  private Schema loadAvroSchema(Configuration conf, Path path) throws IOException {
    Schema avroSchema = getAvroSchemaFromParquetFile(path, conf);

    if (avroSchema == null) {
      throw new RuntimeException("Could not load Avro schema from path: " + path);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Avro schema loaded: " + avroSchema);
    }

    return avroSchema;
  }

  /**
   * This method ensures that the Avro schema in the new path is compatible with the Avro schema in the old path.
   */
  private void validateNewPathAvroSchema(Schema newPathAvroSchema, Schema avroSchema) {
    // If the new path is an empty directory (e.g. in case of a sqoop merge command) then the newPathAvroSchema will
    // be null. In that case we just want to proceed without real validation.
    if (newPathAvroSchema == null) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(format("Validation Avro schema %s against %s", newPathAvroSchema.toString(), avroSchema.toString()));
    }
    SchemaValidator schemaValidator = new SchemaValidatorBuilder().mutualReadStrategy().validateAll();
    try {
      schemaValidator.validate(newPathAvroSchema, singleton(avroSchema));
    } catch (SchemaValidationException e) {
      throw new RuntimeException("Cannot merge files, the Avro schemas are not compatible.", e);
    }
  }

}
