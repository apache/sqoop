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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.db.DBOutputFormat;

import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.ExportJobContext;
import com.google.common.base.Strings;
import org.apache.sqoop.mapreduce.parquet.ParquetExportJobConfigurator;

/**
 * Run an export using JDBC (JDBC-based ExportCallOutputFormat) to
 * call the stored procedure.
 */
public class JdbcCallExportJob extends JdbcExportJob {
  public static final String SQOOP_EXPORT_CALL_KEY = "sqoop.export.call";

  public static final Log LOG = LogFactory.getLog(
      JdbcCallExportJob.class.getName());

  public JdbcCallExportJob(final ExportJobContext context, final ParquetExportJobConfigurator parquetExportJobConfigurator) {
    super(context, null, null, ExportCallOutputFormat.class, parquetExportJobConfigurator);
  }

  public JdbcCallExportJob(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass,
      final ParquetExportJobConfigurator parquetExportJobConfigurator) {
    super(ctxt, mapperClass, inputFormatClass, outputFormatClass, parquetExportJobConfigurator);
  }

  /**
   * makes sure the job knows what stored procedure to call.
   */
  @Override
  protected void propagateOptionsToJob(Job job) {
    super.propagateOptionsToJob(job);
    job.getConfiguration().set(SQOOP_EXPORT_CALL_KEY, options.getCall());
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws IOException {
    String procedureName = job.getConfiguration().get(SQOOP_EXPORT_CALL_KEY);

    ConnManager mgr = context.getConnManager();
    try {
      if (Strings.isNullOrEmpty(options.getUsername())) {
        DBConfiguration.configureDB(job.getConfiguration(),
            mgr.getDriverClass(),
            options.getConnectString(),
            options.getConnectionParams());
      } else {
        DBConfiguration.configureDB(job.getConfiguration(),
            mgr.getDriverClass(),
            options.getConnectString(),
            options.getUsername(),
            options.getPassword(),
            options.getConnectionParams());
      }

      String [] colNames = options.getColumns();
      if (null == colNames) {
        colNames = mgr.getColumnNamesForProcedure(procedureName);
      }
      DBOutputFormat.setOutput(
        job,
        mgr.escapeTableName(procedureName),
        mgr.escapeColNames(colNames));

      job.setOutputFormatClass(getOutputFormatClass());
      job.getConfiguration().set(SQOOP_EXPORT_TABLE_CLASS_KEY, tableClassName);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load OutputFormat", cnfe);
    }
  }

}

