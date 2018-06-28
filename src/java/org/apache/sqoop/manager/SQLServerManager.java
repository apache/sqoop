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

import static org.apache.sqoop.manager.JdbcDrivers.SQLSERVER;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.mapreduce.JdbcUpsertExportJob;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.mapreduce.JdbcExportJob;
import org.apache.sqoop.mapreduce.JdbcUpdateExportJob;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerInputFormat;
import org.apache.sqoop.util.ExportException;
import org.apache.sqoop.util.ImportException;

import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerUpsertOutputFormat;

/**
 * Manages connections to SQLServer databases. Requires the SQLServer JDBC
 * driver.
 */
public class SQLServerManager extends InformationSchemaManager {

  public static final String SCHEMA = "schema";
  public static final String TABLE_HINTS = "table-hints";
  public static final String TABLE_HINTS_PROP
    = "org.apache.sqoop.manager.sqlserver.table.hints";

  public static final Log LOG = LogFactory.getLog(
      SQLServerManager.class.getName());


  // Option to allow inserts on identity columns
  public static final String IDENTITY_INSERT = "identity-insert";
  public static final String IDENTITY_INSERT_PROP =
      "org.apache.sqoop.manager.sqlserver.table.identity";

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

  /**
   * Whether to allow identity inserts.
   */
  private boolean identityInserts;

  final SqlServerManagerContextConfigurator formatConfigurator;

  public SQLServerManager(final SqoopOptions opts) {
    this(SQLSERVER.getDriverClass(), opts);
  }

  public SQLServerManager(final String driver, final SqoopOptions opts) {
    this(driver, opts, new SqlServerManagerContextConfigurator());
  }

  public SQLServerManager(final String driver, final SqoopOptions opts, final SqlServerManagerContextConfigurator configurator) {
    super(driver, opts);
    this.formatConfigurator = configurator;

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
      /*
       * We cannot use the TimeStamp class to represent MS SQL Server
       * datetimeoffset data type since it does not preserve time zone
       * offset values, so use String instead which would work for
       * import/export.
       */
      javaType = "String";
    } else {
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
  public void importTable(org.apache.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    // We're the correct connection manager
    context.setConnManager(this);

    // Propagate table hints to job
    Configuration configuration = context.getOptions().getConf();
    if (tableHints != null) {
      configuration.set(TABLE_HINTS_PROP, tableHints);
    }
    String splitColumn = getSplitColumn(context.getOptions(), context.getTableName());
    context.setInputFormat(SqlServerInputFormat.class);
    formatConfigurator.configureContextForImport(context, splitColumn);
    super.importTable(context);
  }

  /**
   * Export data stored in HDFS into a table in a database.
   */
  @Override
  public void exportTable(org.apache.sqoop.manager.ExportJobContext context) throws IOException, ExportException {
    context.setConnManager(this);

    // Propagate table hints to job
    Configuration configuration = context.getOptions().getConf();
    if (tableHints != null) {
      configuration.set(TABLE_HINTS_PROP, tableHints);
    }

    // Propagate whether to allow identity inserts to job
    configuration.setBoolean(IDENTITY_INSERT_PROP, identityInserts);

    formatConfigurator.configureContextForExport(context);
    JdbcExportJob exportJob = new JdbcExportJob(context, null, null,
        context.getOutputFormatClass(), getParquetJobConfigurator().createParquetExportJobConfigurator());
    exportJob.runExport();
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void updateTable(org.apache.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    boolean runAsExportJob = formatConfigurator.configureContextForUpdate(context, this);
    if (runAsExportJob) {
      JdbcUpdateExportJob exportJob = new JdbcUpdateExportJob(context, null,
          null, context.getOutputFormatClass(), getParquetJobConfigurator().createParquetExportJobConfigurator());
      exportJob.runExport();
    } else {
      super.updateTable(context);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void upsertTable(org.apache.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);

    // Propagate table hints to job
    Configuration configuration = context.getOptions().getConf();
    if (tableHints != null) {
      configuration.set(TABLE_HINTS_PROP, tableHints);
    }

    JdbcUpsertExportJob exportJob =
        new JdbcUpsertExportJob(context, SqlServerUpsertOutputFormat.class, getParquetJobConfigurator().createParquetExportJobConfigurator());
    exportJob.runExport();
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void configureDbOutputColumns(SqoopOptions options) {
    if (options.getUpdateMode() == SqoopOptions.UpdateMode.UpdateOnly) {
      super.configureDbOutputColumns(options);
    } else {
      // We're in upsert mode. We need to explicitly set
      // the database output column ordering in the codeGenerator.
      options.setDbOutputColumns(getColumnNames(options.getTableName()));
    }
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
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
    + "WHERE TABLE_SCHEMA = (" + getSchemaQuery() + ") "
    + "  AND TABLE_NAME = N'" + tableName + "' "
    + "  ORDER BY ORDINAL_POSITION";
  }

  @Override
  protected String getPrimaryKeyQuery(String tableName) {
    return
      "SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc, "
    + "  INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
    + "WHERE tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
    + "  AND tc.TABLE_NAME = kcu.TABLE_NAME "
    + "  AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
    + "  AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
    + "  AND tc.TABLE_SCHEMA = (" + getSchemaQuery() + ") "
    + "  AND tc.TABLE_NAME = N'" + tableName + "' "
    + "  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'";
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

    if (cmdLine.hasOption(SqlServerManagerContextConfigurator.RESILIENT_OPTION)) {
      LOG.warn("Sqoop will use resilient operations! In case of import, " +
          "the split-by column also has to be specified, unique, and in ascending order.");
    }

    identityInserts = cmdLine.hasOption(IDENTITY_INSERT);
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

    extraOptions.addOption(OptionBuilder
      .withDescription("Allow identity inserts")
      .withLongOpt(IDENTITY_INSERT).create());

    extraOptions.addOption(OptionBuilder
        .withLongOpt(SqlServerManagerContextConfigurator.RESILIENT_OPTION).create());

    return extraOptions;
  }

  /**
   * Launch a MapReduce job via DataDrivenImportJob to read the table with
   * SQLServerDBInputFormat which handles connection failures while
   * using free-form query importer.
   */
  public void importQuery(org.apache.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    String splitColumn = getSplitColumn(context.getOptions(), context.getTableName());
    formatConfigurator.configureContextForImport(context, splitColumn);
    super.importQuery(context);
  }

}

