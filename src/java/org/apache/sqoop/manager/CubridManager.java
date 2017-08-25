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
package org.apache.sqoop.manager;

import static org.apache.sqoop.manager.JdbcDrivers.CUBRID;

import java.io.IOException;
import java.sql.Types;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.mapreduce.cubrid.CubridUpsertOutputFormat;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.mapreduce.ExportBatchOutputFormat;
import com.cloudera.sqoop.mapreduce.JdbcExportJob;
import com.cloudera.sqoop.mapreduce.JdbcUpsertExportJob;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * Manages connections to CUBRID databases.
 */
public class CubridManager extends
    com.cloudera.sqoop.manager.CatalogQueryManager {

  public static final Log LOG = LogFactory
      .getLog(CubridManager.class.getName());

  public CubridManager(final SqoopOptions opts) {
    super(CUBRID.getDriverClass(), opts);
  }

  @Override
  public void importTable(
      com.cloudera.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {

    // Then run the normal importTable() method.
    super.importTable(context);
  }

  /**
   * Export data stored in HDFS into a table in a database.
   */
  public void exportTable(
      com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    JdbcExportJob exportJob = new JdbcExportJob(context, null, null,
        ExportBatchOutputFormat.class);

    exportJob.runExport();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void upsertTable(
      com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);

    JdbcUpsertExportJob exportJob = new JdbcUpsertExportJob(context,
        CubridUpsertOutputFormat.class);
    exportJob.runExport();
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void configureDbOutputColumns(SqoopOptions options) {
    // In case that we're running upsert, we do not want to
    // change column order as we're actually going to use
    // INSERT INTO ... ON DUPLICATE KEY UPDATE
    // clause.
    if (options.getUpdateMode()
        == SqoopOptions.UpdateMode.AllowInsert) {
      return;
    }

    super.configureDbOutputColumns(options);
  }

  @Override
  public String getColNamesQuery(String tableName) {
    // Use LIMIT to return fast
    return "SELECT t.* FROM " + escapeTableName(tableName)
        + " AS t LIMIT 1";
  }

  @Override
  public String getInputBoundsQuery(String splitByCol,
      String sanitizedQuery) {
    return "SELECT MIN(" + splitByCol + "), MAX("
      + splitByCol + ") FROM ("
      + sanitizedQuery + ") t1";
  }

  private Map<String, String> colTypeNames;
  private static final int YEAR_TYPE_OVERWRITE = Types.SMALLINT;

  private int overrideSqlType(String tableName, String columnName,
      int sqlType) {

    if (colTypeNames == null) {
      colTypeNames = getColumnTypeNames(tableName,
          options.getCall(),
          options.getSqlQuery());
    }

    if ("YEAR".equalsIgnoreCase(colTypeNames.get(columnName))) {
      sqlType = YEAR_TYPE_OVERWRITE;
    }
    return sqlType;
  }

  @Override
  public String toJavaType(String tableName, String columnName,
      int sqlType) {
    sqlType = overrideSqlType(tableName, columnName, sqlType);
    String javaType = super.toJavaType(tableName,
        columnName, sqlType);
    return javaType;
  }

  @Override
  public String toHiveType(String tableName, String columnName,
      int sqlType) {
    sqlType = overrideSqlType(tableName, columnName, sqlType);
    return super.toHiveType(tableName, columnName, sqlType);
  }

  @Override
  public Type toAvroType(String tableName, String columnName,
      int sqlType) {
    sqlType = overrideSqlType(tableName, columnName, sqlType);
    return super.toAvroType(tableName, columnName, sqlType);
  }

  @Override
  protected String getListDatabasesQuery() {
    return null;
  }

  @Override
  protected String getListTablesQuery() {
    return "SELECT CLASS_NAME FROM DB_CLASS WHERE"
      + " IS_SYSTEM_CLASS = 'NO'";
  }

  @Override
  protected String getListColumnsQuery(String tableName) {
    tableName = tableName.toLowerCase();
    return "SELECT ATTR_NAME FROM DB_ATTRIBUTE WHERE"
        + " CLASS_NAME = '"
        + tableName + "'  ORDER BY def_order";
  }

  @Override
  protected String getPrimaryKeyQuery(String tableName) {
    tableName = tableName.toLowerCase();
    return "SELECT KEY_ATTR_NAME FROM DB_INDEX_KEY WHERE"
        + " CLASS_NAME = '"
        + tableName + "' ";
  }

}
