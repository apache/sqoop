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
import com.cloudera.sqoop.mapreduce.ExportBatchOutputFormat;
import com.cloudera.sqoop.mapreduce.JdbcExportJob;
import com.cloudera.sqoop.util.ExportException;
import org.apache.sqoop.cli.RelatedOptions;

/**
 * Manages connections to SQLServer databases. Requires the SQLServer JDBC
 * driver.
 */
public class SQLServerManager
    extends com.cloudera.sqoop.manager.InformationSchemaManager {

  public static final String SCHEMA = "schema";

  public static final Log LOG = LogFactory.getLog(
      SQLServerManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS =
      "com.microsoft.sqlserver.jdbc.SQLServerDriver";

  /**
   * Schema name that we will use.
   */
  private String schema;

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
   * Export data stored in HDFS into a table in a database.
   */
  @Override
  public void exportTable(com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    JdbcExportJob exportJob = new JdbcExportJob(context, null, null,
      ExportBatchOutputFormat.class);
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

    return extraOptions;
  }
}

