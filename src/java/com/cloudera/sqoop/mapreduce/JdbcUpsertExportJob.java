/**
 * Copyright 2011 The Apache Software Foundation
 *
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

package com.cloudera.sqoop.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBOutputFormat;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ExportJobContext;

/**
 * Run an update/insert export using JDBC (JDBC-based UpsertOutputFormat).
 */
public class JdbcUpsertExportJob extends JdbcUpdateExportJob {

  public static final Log LOG = LogFactory.getLog(
      JdbcUpsertExportJob.class.getName());

  public JdbcUpsertExportJob(final ExportJobContext context,
      final Class<? extends OutputFormat> outputFormatClass)
      throws IOException {
    super(context, null, null, outputFormatClass);
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws IOException {

    ConnManager mgr = context.getConnManager();
    try {
      String username = options.getUsername();
      if (null == username || username.length() == 0) {
        DBConfiguration.configureDB(job.getConfiguration(),
            mgr.getDriverClass(),
            options.getConnectString());
      } else {
        DBConfiguration.configureDB(job.getConfiguration(),
            mgr.getDriverClass(),
            options.getConnectString(),
            username, options.getPassword());
      }

      String [] colNames = options.getColumns();
      if (null == colNames) {
        colNames = mgr.getColumnNames(tableName);
      }
      if (null == colNames) {
        throw new IOException(
            "Export column names could not be determined for " + tableName);
      }
      DBOutputFormat.setOutput(job, tableName, colNames);

      String updateKeyCol = options.getUpdateKeyCol();
      if (null == updateKeyCol) {
        throw new IOException("Update key column not set in export job");
      }

      job.setOutputFormatClass(getOutputFormatClass());
      job.getConfiguration().set(SQOOP_EXPORT_TABLE_CLASS_KEY, tableClassName);
      job.getConfiguration().set(SQOOP_EXPORT_UPDATE_COL_KEY, updateKeyCol);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load OutputFormat", cnfe);
    }
  }
}

