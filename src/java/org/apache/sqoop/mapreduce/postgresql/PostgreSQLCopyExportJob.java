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

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.manager.ExportJobContext;
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
import org.apache.sqoop.mapreduce.parquet.ParquetExportJobConfigurator;


/**
 * Run an export using PostgreSQL JDBC Copy API.
 */
public class PostgreSQLCopyExportJob extends JdbcExportJob {
  public static final Log LOG =
    LogFactory.getLog(PostgreSQLCopyExportJob.class.getName());

  public PostgreSQLCopyExportJob(final ExportJobContext context, final ParquetExportJobConfigurator parquetExportJobConfigurator) {
    super(context, parquetExportJobConfigurator);
  }

  public PostgreSQLCopyExportJob(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass,
      final ParquetExportJobConfigurator parquetExportJobConfigurator) {
    super(ctxt, mapperClass, inputFormatClass, outputFormatClass, parquetExportJobConfigurator);
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
/* list of chars that cannot be passed via jobconf */
  final static String badXmlString = "\u0000\u0001\u0002\u0003\u0004\u0005" +
    "\u0006\u0007\u0008\u000B\u000C\u000E\u000F\u0010\u0011\u0012" +
    "\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C" +
    "\u001D\u001E\u001F\uFFFE\uFFFF";

/* true if the char is ok to pass via Configuration */
  public static boolean validXml(char s){
    return (badXmlString.indexOf(s)<0);
  }


  protected void propagateOptionsToJob(Job job) {
    super.propagateOptionsToJob(job);
    SqoopOptions opts = context.getOptions();
    Configuration conf = job.getConfiguration();

    /* empty string needs to be passed as a flag */
    if ("".equals(opts.getNullStringValue())) {
       conf.set("postgresql.null.emptystring","true");
    }

    /* valid delimiters may not be valid xml chars, so the hadoop conf will fail.
     * but we still want to support them so we base64 encode it in that case
     * */
    char  delim= opts.getInputFieldDelim();
    String delimString=Character.toString(delim);
    if(validXml(delim)){ 
      setDelimiter("postgresql.input.field.delim",delim,conf);
    }else{
      conf.set("postgresql.input.field.delim.base64",
          java.util.Base64.getEncoder().encodeToString(delimString.getBytes()));
    }

   /* use the --batch switch to enable line buffering */
   if (opts.isBatchMode()){
       conf.set("postgresql.export.batchmode","true");
   }

/* todo: there may still be some case where user wants an invalid xml char for record delim */
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
