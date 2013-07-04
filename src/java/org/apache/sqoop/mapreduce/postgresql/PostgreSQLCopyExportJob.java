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

package org.apache.sqoop.mapreduce.postgresql;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.manager.ExportJobContext;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.mapreduce.JdbcExportJob;



/**
 * Run an export using PostgreSQL JDBC Copy API.
 */
public class PostgreSQLCopyExportJob extends JdbcExportJob {
  public static final Log LOG =
    LogFactory.getLog(PostgreSQLCopyExportJob.class.getName());

  public PostgreSQLCopyExportJob(final ExportJobContext context) {
    super(context);
  }

  public PostgreSQLCopyExportJob(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {
    super(ctxt, mapperClass, inputFormatClass, outputFormatClass);
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    return PostgreSQLCopyExportMapper.class;
  }

  @Override
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    if (isHCatJob) {
      throw new IOException("Sqoop-HCatalog Integration is not supported.");
    }
    switch (getInputFileType()) {
      case AVRO_DATA_FILE:
        throw new IOException("Avro data file is not supported.");
      case SEQUENCE_FILE:
      case UNKNOWN:
      default:
        job.setMapperClass(getMapperClass());
    }

    // Concurrent writes of the same records would be problematic.
    ConfigurationHelper.setJobMapSpeculativeExecution(job, false);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
  }

  protected void propagateOptionsToJob(Job job) {
    super.propagateOptionsToJob(job);
    SqoopOptions opts = context.getOptions();
    Configuration conf = job.getConfiguration();
    if (opts.getNullStringValue() != null) {
      conf.set("postgresql.null.string", opts.getNullStringValue());
    }
    setDelimiter("postgresql.input.field.delim",
                 opts.getInputFieldDelim(), conf);
    setDelimiter("postgresql.input.record.delim",
                 opts.getInputRecordDelim(), conf);
    setDelimiter("postgresql.input.enclosedby",
                 opts.getInputEnclosedBy(), conf);
    setDelimiter("postgresql.input.escapedby",
                 opts.getInputEscapedBy(), conf);
    conf.setBoolean("postgresql.input.encloserequired",
                    opts.isInputEncloseRequired());
  }

  private void setDelimiter(String prop, char val, Configuration conf) {
    switch (val) {
    case DelimiterSet.NULL_CHAR:
      break;
    case '\t':
    default:
      conf.set(prop, String.valueOf(val));
    }
  }
}
