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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.util.ImportException;

/**
 * Manages connections to Postgresql databases.
 */
public class PostgresqlManager
    extends com.cloudera.sqoop.manager.CatalogQueryManager {

  public static final Log LOG = LogFactory.getLog(
      PostgresqlManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS = "org.postgresql.Driver";

  // set to true after we warn the user that we can use direct fastpath.
  private static boolean warningPrinted = false;

  public PostgresqlManager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  protected PostgresqlManager(final SqoopOptions opts, boolean ignored) {
    // constructor used by subclasses to avoid the --direct warning.
    super(DRIVER_CLASS, opts);
  }

  @Override
  public String escapeColName(String colName) {
    return escapeIdentifier(colName);
  }

  @Override
  public String escapeTableName(String tableName) {
    return escapeIdentifier(tableName);
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
      this.getConnection().commit(); // Commit any changes made thus far.
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
    + "WHERE SCHEMANAME = (SELECT CURRENT_SCHEMA())";
  }

  @Override
  protected String getListColumnsQuery(String tableName) {
    return
      "SELECT col.ATTNAME FROM PG_CATALOG.PG_NAMESPACE sch,"
    + "  PG_CATALOG.PG_CLASS tab, PG_CATALOG.PG_ATTRIBUTE col "
    + "WHERE sch.OID = tab.RELNAMESPACE "
    + "  AND tab.OID = col.ATTRELID "
    + "  AND sch.NSPNAME = (SELECT CURRENT_SCHEMA()) "
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
    + "  AND sch.NSPNAME = (SELECT CURRENT_SCHEMA()) "
    + "  AND tab.RELNAME = '" + escapeLiteral(tableName) + "' "
    + "  AND col.ATTNUM = ANY(ind.INDKEY) "
    + "  AND ind.INDISPRIMARY";
  }

  private String escapeLiteral(String literal) {
    return literal.replace("'", "''");
  }

  @Override
  protected String getCurTimestampQuery() {
    return "SELECT CURRENT_TIMESTAMP";
  }

}

