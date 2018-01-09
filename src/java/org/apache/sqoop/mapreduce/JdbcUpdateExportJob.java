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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.mapreduce.ExportJobBase.FileType;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;

import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.ExportJobContext;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.db.DBOutputFormat;
import org.apache.sqoop.util.FileSystemUtil;

/**
 * Run an update-based export using JDBC (JDBC-based UpdateOutputFormat).
 */
public class JdbcUpdateExportJob extends ExportJobBase {

  // Fix For Issue [SQOOP-2846]
  private FileType fileType;
  public static final Log LOG = LogFactory.getLog(
      JdbcUpdateExportJob.class.getName());

  /**
   * Return an instance of the UpdateOutputFormat class object loaded
   * from the shim jar.
   */
  private static Class<? extends OutputFormat> getUpdateOutputFormat()
       throws IOException {
    return UpdateOutputFormat.class;
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

  // Fix For Issue [SQOOP-2846]
  @Override
  protected Class<? extends Mapper> getMapperClass() {
    if (isHCatJob) {
      return SqoopHCatUtilities.getExportMapperClass();
    }
    switch (fileType) {
    case SEQUENCE_FILE:
      return SequenceFileExportMapper.class;
    case AVRO_DATA_FILE:
      return AvroExportMapper.class;
    case PARQUET_FILE:
      return ParquetExportMapper.class;
    case UNKNOWN:
    default:
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
            options.getConnectString(),
            options.getConnectionParams());
      } else {
        DBConfiguration.configureDB(job.getConfiguration(),
            mgr.getDriverClass(),
            options.getConnectString(),
            username, options.getPassword(),
            options.getConnectionParams());
      }

      String [] colNames = options.getColumns();
      if (null == colNames) {
        colNames = mgr.getColumnNames(tableName);
      }

      if (null == colNames) {
        throw new IOException(
            "Export column names could not be determined for " + tableName);
      }

      String updateKeyColumns = options.getUpdateKeyCol();
      if (null == updateKeyColumns) {
        throw new IOException("Update key column not set in export job");
      }
      // Update key columns lookup and removal
      Set<String> updateKeys = new LinkedHashSet<String>();
      Set<String> updateKeysUppercase = new HashSet<String>();
      StringTokenizer stok = new StringTokenizer(updateKeyColumns, ",");
      while (stok.hasMoreTokens()) {
        String nextUpdateKey = stok.nextToken().trim();
        if (nextUpdateKey.length() > 0) {
          updateKeys.add(nextUpdateKey);
          updateKeysUppercase.add(nextUpdateKey.toUpperCase());
        }  else {
          throw new RuntimeException("Invalid update key column value specified"
              + ": '" + updateKeyColumns + "'");
        }
      }

      if (updateKeys.size() == 0) {
        throw new IOException("Update key columns not valid in export job");
      }

      // Make sure we strip out the key column from this list.
      String [] outColNames = new String[colNames.length - updateKeys.size()];
      int j = 0;
      for (int i = 0; i < colNames.length; i++) {
        if (!updateKeysUppercase.contains(colNames[i].toUpperCase())) {
          outColNames[j++] = colNames[i];
        }
      }
      DBOutputFormat.setOutput(job, mgr.escapeTableName(tableName), mgr.escapeColNames(outColNames));

      job.setOutputFormatClass(getOutputFormatClass());
      job.getConfiguration().set(SQOOP_EXPORT_TABLE_CLASS_KEY, tableClassName);
      job.getConfiguration().set(SQOOP_EXPORT_UPDATE_COL_KEY, updateKeyColumns);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load OutputFormat", cnfe);
    }
  }

  // Fix For Issue [SQOOP-2846]
  @Override
  protected void configureInputFormat(Job job, String tableName, String tableClassName,
      String splitByCol)
          throws ClassNotFoundException, IOException
  {

    fileType = getInputFileType();

    super.configureInputFormat(job, tableName, tableClassName, splitByCol);

    if (isHCatJob) {
      SqoopHCatUtilities.configureExportInputFormat(options, job, context.getConnManager(),
          tableName,
          job.getConfiguration());
      return;
    } else if (fileType == FileType.AVRO_DATA_FILE) {
      LOG.debug("Configuring for Avro export");
      configureGenericRecordExportInputFormat(job, tableName);
    } else if (fileType == FileType.PARQUET_FILE) {
      LOG.debug("Configuring for Parquet export");
      configureGenericRecordExportInputFormat(job, tableName);
      String uri = "dataset:" + FileSystemUtil.makeQualified(getInputPath(), job.getConfiguration());
      DatasetKeyInputFormat.configure(job).readFrom(uri);
    }
  }

  // Fix For Issue [SQOOP-2846]
  private void configureGenericRecordExportInputFormat(Job job, String tableName)
      throws IOException
  {
    ConnManager connManager = context.getConnManager();
    Map<String, Integer> columnTypeInts;
    if (options.getCall() == null) {
      columnTypeInts = connManager.getColumnTypes(tableName, options.getSqlQuery());
    } else {
      columnTypeInts = connManager.getColumnTypesForProcedure(options.getCall());
    }
    MapWritable columnTypes = new MapWritable();
    for (Map.Entry<String, Integer> e : columnTypeInts.entrySet()) {
      Text columnName = new Text(e.getKey());
      Text columnText = new Text(connManager.toJavaType(tableName, e.getKey(), e.getValue()));
      columnTypes.put(columnName, columnText);
    }
    DefaultStringifier.store(job.getConfiguration(), columnTypes,
        AvroExportMapper.AVRO_COLUMN_TYPES_MAP);
  }

  // Fix For Issue [SQOOP-2846]
  @Override
  protected Class<? extends InputFormat> getInputFormatClass() throws ClassNotFoundException {
    if (isHCatJob) {
      return SqoopHCatUtilities.getInputFormatClass();
    }
    switch (fileType) {
    case AVRO_DATA_FILE:
      return AvroInputFormat.class;
    case PARQUET_FILE:
      return DatasetKeyInputFormat.class;
    default:
      return super.getInputFormatClass();
    }
  }
}
