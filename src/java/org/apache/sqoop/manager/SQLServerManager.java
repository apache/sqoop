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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.mapreduce.JdbcExportJob;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerExportBatchOutputFormat;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerInputFormat;

/**
 * Manages connections to SQLServer databases. Requires the SQLServer JDBC
 * driver.
 */
public class SQLServerManager
    extends com.cloudera.sqoop.manager.InformationSchemaManager {

  public static final String SCHEMA = "schema";
  public static final String TABLE_HINTS = "table-hints";
  public static final String TABLE_HINTS_PROP
    = "org.apache.sqoop.manager.sqlserver.table.hints";

  public static final Log LOG = LogFactory.getLog(
      SQLServerManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS =
      "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  // Define SQL Server specific types that are not covered by parent classes
  private static final int DATETIMEOFFSET = -155;

  /**
   * Schema name that we will use.
   */
  private String schema;

  /**
   * Optional table hints to use.
   */
  private String tableHints;

  public SQLServerManager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);

    // Try to parse extra arguments
    try {
      parseExtraArgs(opts.getExtraArgs());
    } catch (ParseException e) {
      throw new RuntimeException("Can't parse extra arguments", e);
    }
  }

  /**
   * Resolve MS SQL Server specific database type to the Java type that should
   * contain it.
   * @param sqlType     sql type
   * @return the name of a Java type to hold the sql datatype, or null if none.
   */
  @Override
  public String toJavaType(int sqlType) {
    String javaType;

    if (sqlType == DATETIMEOFFSET) {
      // We cannot use the TimeStamp class to represent MS SQL Server datetimeoffset
      // data type since it does not preserve time zone offset values, so use String
      // instead which would work for import/export
      javaType = "String";
    }else {
      //If none of the above data types match, it returns parent method's
      //status, which can be null.
      javaType = super.toJavaType(sqlType);
    }
    return javaType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void importTable(
          com.cloudera.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    // We're the correct connection manager
    context.setConnManager(this);

    // Propagate table hints to job
    Configuration configuration = context.getOptions().getConf();
    if (tableHints != null) {
      configuration.set(TABLE_HINTS_PROP, tableHints);
    }

    // Set our own input format
    context.setInputFormat(SqlServerInputFormat.class);
    super.importTable(context);
  }

  /**
   * Export data stored in HDFS into a table in a database.
   */
  @Override
  public void exportTable(com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);

    // Propagate table hints to job
    Configuration configuration = context.getOptions().getConf();
    if (tableHints != null) {
      configuration.set(TABLE_HINTS_PROP, tableHints);
    }

    JdbcExportJob exportJob = new JdbcExportJob(context, null, null,
      SqlServerExportBatchOutputFormat.class);
    exportJob.runExport();
  }

  /**
   * SQLServer does not support the CURRENT_TIMESTAMP() function. Instead
   * it has the notion of keyword CURRENT_TIMESTAMP that resolves to the
   * current time stamp for the database system.
   */
  @Override
  public String getCurTimestampQuery() {
      return "SELECT CURRENT_TIMESTAMP";
  }

  @Override
  protected String getListDatabasesQuery() {
    return "SELECT NAME FROM SYS.DATABASES";
  }

  @Override
  protected String getSchemaQuery() {
    if (schema == null) {
      return "SELECT SCHEMA_NAME()";
    }

    return "'" + schema + "'";
  }

  @Override
  protected String getListColumnsQuery(String tableName) {
    return
      super.getListColumnsQuery(tableName)
    + "  ORDER BY ORDINAL_POSITION";
  }
  
  @Override
  public String escapeColName(String colName) {
    return escapeObjectName(colName);
  }

  @Override
  public String escapeTableName(String tableName) {
    // Return table name including schema if requested
    if (schema != null && !schema.isEmpty()) {
      return escapeObjectName(schema) + "." + escapeObjectName(tableName);
    }

    return escapeObjectName(tableName);
  }

  @Override
  public boolean escapeTableNameOnExport() {
    return true;
  }

  /**
   * Escape database object name (database, table, column, schema).
   *
   * @param objectName Object name in database
   * @return Escaped variant of the name
   */
  public String escapeObjectName(String objectName) {
    if (null == objectName) {
      return null;
    }
    return "[" + objectName + "]";
  }

  /**
   * Parse extra arguments.
   *
   * @param args Extra arguments array
   * @throws ParseException
   */
  void parseExtraArgs(String[] args) throws ParseException {
    // No-op when no extra arguments are present
    if (args == null || args.length == 0) {
      return;
    }

    // We do not need extended abilities of SqoopParser, so we're using
    // Gnu parser instead.
    CommandLineParser parser = new GnuParser();
    CommandLine cmdLine = parser.parse(getExtraOptions(), args, true);

    // Apply extra options
    if (cmdLine.hasOption(SCHEMA)) {
      String schemaName = cmdLine.getOptionValue(SCHEMA);
      LOG.info("We will use schema " + schemaName);

      this.schema = schemaName;
    }

    // Apply table hints
    if (cmdLine.hasOption(TABLE_HINTS)) {
      String hints = cmdLine.getOptionValue(TABLE_HINTS);
      LOG.info("Sqoop will use following table hints for data transfer: "
        + hints);

      this.tableHints = hints;
    }
  }

  /**
   * Create related options for SQL Server extra parameters.
   *
   * @return
   */
  @SuppressWarnings("static-access")
  private RelatedOptions getExtraOptions() {
    // Connection args (common)
    RelatedOptions extraOptions =
      new RelatedOptions("SQL Server extra options:");

    extraOptions.addOption(OptionBuilder.withArgName("string").hasArg()
      .withDescription("Optional schema name")
      .withLongOpt(SCHEMA).create());

    extraOptions.addOption(OptionBuilder.withArgName("string").hasArg()
      .withDescription("Optional table hints to use")
      .withLongOpt(TABLE_HINTS).create());

    return extraOptions;
  }
}

