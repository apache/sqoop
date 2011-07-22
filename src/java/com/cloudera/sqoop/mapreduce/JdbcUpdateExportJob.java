/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBOutputFormat;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.shims.ShimLoader;

/**
 * Run an update-based export using JDBC (JDBC-based UpdateOutputFormat).
 */
public class JdbcUpdateExportJob extends ExportJobBase {

  public static final Log LOG = LogFactory.getLog(
      JdbcUpdateExportJob.class.getName());

  /**
   * Return an instance of the UpdateOutputFormat class object loaded
   * from the shim jar.
   */
  private static Class<? extends OutputFormat> getUpdateOutputFormat()
       throws IOException {
    try {
      return (Class<? extends OutputFormat>) ShimLoader.getShimClass(
          "com.cloudera.sqoop.mapreduce.UpdateOutputFormat");
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load updating export OutputFormat",
          cnfe);
    }
  }

  public JdbcUpdateExportJob(final ExportJobContext context)
      throws IOException {
    super(context, null, null, getUpdateOutputFormat());
  }

  public JdbcUpdateExportJob(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {
    super(ctxt, mapperClass, inputFormatClass, outputFormatClass);
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    if (inputIsSequenceFiles()) {
      return SequenceFileExportMapper.class;
    } else {
      return TextExportMapper.class;
    }
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

      String updateKeyCol = options.getUpdateKeyCol();
      if (null == updateKeyCol) {
        throw new IOException("Update key column not set in export job");
      }

      // Make sure we strip out the key column from this list.
      String [] outColNames = new String[colNames.length - 1];
      int j = 0;
      String upperCaseKeyCol = updateKeyCol.toUpperCase();
      for (int i = 0; i < colNames.length; i++) {
        if (!colNames[i].toUpperCase().equals(upperCaseKeyCol)) {
          outColNames[j++] = colNames[i];
        }
      }
      DBOutputFormat.setOutput(job, tableName, outColNames);

      job.setOutputFormatClass(getOutputFormatClass());
      job.getConfiguration().set(SQOOP_EXPORT_TABLE_CLASS_KEY, tableClassName);
      job.getConfiguration().set(SQOOP_EXPORT_UPDATE_COL_KEY, updateKeyCol);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load OutputFormat", cnfe);
    }
  }
}

