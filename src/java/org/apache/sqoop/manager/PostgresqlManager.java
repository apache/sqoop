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
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.util.ImportException;
import org.apache.sqoop.cli.RelatedOptions;

/**
 * Manages connections to Postgresql databases.
 */
public class PostgresqlManager
    extends com.cloudera.sqoop.manager.CatalogQueryManager {

  public static final String SCHEMA = "schema";

  public static final Log LOG = LogFactory.getLog(
      PostgresqlManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS = "org.postgresql.Driver";

  // set to true after we warn the user that we can use direct fastpath.
  private static boolean warningPrinted = false;

  /*
   * PostgreSQL schema that we should use.
   */
  private String schema;

  public PostgresqlManager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);

    // Try to parse extra arguments
    try {
      parseExtraArgs(opts.getExtraArgs());
    } catch (ParseException e) {
      throw new RuntimeException("Can't parse extra arguments", e);
    }
  }

  @Override
  public String escapeColName(String colName) {
    return escapeIdentifier(colName);
  }

  @Override
  public String escapeTableName(String tableName) {
    // Return full table name including schema if needed
    if (schema != null && !schema.isEmpty()) {
      return escapeIdentifier(schema) + "." + escapeIdentifier(tableName);
    }

    return escapeIdentifier(tableName);
  }

  @Override
  public boolean escapeTableNameOnExport() {
    return true;
  }

  protected String escapeIdentifier(String identifier) {
    if (identifier == null) {
      return null;
    }
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  @Override
  public void close() throws SQLException {
    if (this.hasOpenConnection()) {
      this.getConnection().rollback(); // rollback any pending changes.
    }

    super.close();
  }

  @Override
  protected String getColNamesQuery(String tableName) {
    // Use LIMIT to return fast
    return "SELECT t.* FROM " + escapeTableName(tableName) + " AS t LIMIT 1";
  }

  @Override
  public void importTable(
          com.cloudera.sqoop.manager.ImportJobContext context)
        throws IOException, ImportException {

    // The user probably should have requested --direct to invoke pg_dump.
    // Display a warning informing them of this fact.
    if (!PostgresqlManager.warningPrinted) {
      LOG.warn("It looks like you are importing from postgresql.");
      LOG.warn("This transfer can be faster! Use the --direct");
      LOG.warn("option to exercise a postgresql-specific fast path.");

      PostgresqlManager.warningPrinted = true; // don't display this twice.
    }

    // Then run the normal importTable() method.
    super.importTable(context);
  }

  @Override
  public boolean supportsStagingForExport() {
    return true;
  }

  @Override
  protected String getListDatabasesQuery() {
    return
      "SELECT DATNAME FROM PG_CATALOG.PG_DATABASE";
  }

  @Override
  protected String getListTablesQuery() {
    return
      "SELECT TABLENAME FROM PG_CATALOG.PG_TABLES "
    + "WHERE SCHEMANAME = " + getSchemaSqlFragment();
  }

  @Override
  protected String getListColumnsQuery(String tableName) {
    return
      "SELECT col.ATTNAME FROM PG_CATALOG.PG_NAMESPACE sch,"
    + "  PG_CATALOG.PG_CLASS tab, PG_CATALOG.PG_ATTRIBUTE col "
    + "WHERE sch.OID = tab.RELNAMESPACE "
    + "  AND tab.OID = col.ATTRELID "
    + "  AND sch.NSPNAME = " + getSchemaSqlFragment()
    + "  AND tab.RELNAME = '" + escapeLiteral(tableName) + "' "
    + "  AND col.ATTNUM >= 1"
    + "  AND col.ATTISDROPPED = 'f'";
  }

  @Override
  protected String getPrimaryKeyQuery(String tableName) {
    return
      "SELECT col.ATTNAME FROM PG_CATALOG.PG_NAMESPACE sch, "
    + "  PG_CATALOG.PG_CLASS tab, PG_CATALOG.PG_ATTRIBUTE col, "
    + "  PG_CATALOG.PG_INDEX ind "
    + "WHERE sch.OID = tab.RELNAMESPACE "
    + "  AND tab.OID = col.ATTRELID "
    + "  AND tab.OID = ind.INDRELID "
    + "  AND sch.NSPNAME = " + getSchemaSqlFragment()
    + "  AND tab.RELNAME = '" + escapeLiteral(tableName) + "' "
    + "  AND col.ATTNUM = ANY(ind.INDKEY) "
    + "  AND ind.INDISPRIMARY";
  }

  private String getSchemaSqlFragment() {
    if (schema != null && !schema.isEmpty()) {
      return "'" + escapeLiteral(schema) + "'";
    }

    return "(SELECT CURRENT_SCHEMA())";
  }

  private String escapeLiteral(String literal) {
    return literal.replace("'", "''");
  }

  @Override
  protected String getCurTimestampQuery() {
    return "SELECT CURRENT_TIMESTAMP";
  }

  /**
   * Parse extra arguments.
   *
   * @param args Extra arguments array
   * @throws ParseException
   */
  private void parseExtraArgs(String[] args) throws ParseException {
    // No-op when no extra arguments are present
    if (args == null || args.length == 0) {
      return;
    }

    // We do not need extended abilities of SqoopParser, so we're using
    // Gnu parser instead.
    CommandLineParser parser = new GnuParser();
    CommandLine cmdLine = parser.parse(getExtraOptions(), args, true);

    // Apply parsed arguments
    applyExtraArguments(cmdLine);
  }

  protected void applyExtraArguments(CommandLine cmdLine) {
    // Apply extra options
    if (cmdLine.hasOption(SCHEMA)) {
      String schemaName = cmdLine.getOptionValue(SCHEMA);
      LOG.info("We will use schema " + schemaName);

      this.schema = schemaName;
    }
  }

  /**
   * Create related options for PostgreSQL extra parameters.
   *
   * @return
   */
  @SuppressWarnings("static-access")
  protected RelatedOptions getExtraOptions() {
    // Connection args (common)
    RelatedOptions extraOptions =
      new RelatedOptions("PostgreSQL extra options:");

    extraOptions.addOption(OptionBuilder.withArgName("string").hasArg()
      .withDescription("Optional schema name")
      .withLongOpt(SCHEMA).create());

    return extraOptions;
  }
}

