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

package com.cloudera.sqoop.manager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * Abstract interface that manages connections to a database.
 * The implementations of this class drive the actual discussion with
 * the database about table formats, etc.
 */
public abstract class ConnManager {

  public static final Log LOG = LogFactory.getLog(SqlManager.class.getName());

  /**
   * Return a list of all databases on a server.
   */
  public abstract String [] listDatabases();

  /**
   * Return a list of all tables in a database.
   */
  public abstract String [] listTables();

  /**
   * Return a list of column names in a table in the order returned by the db.
   */
  public abstract String [] getColumnNames(String tableName);

  /**
   * Return a list of column names in query in the order returned by the db.
   */
  public String [] getColumnNamesForQuery(String query) {
    LOG.error("This database does not support free-form query column names.");
    return null;
  }

  /**
   * Return the name of the primary key for a table, or null if there is none.
   */
  public abstract String getPrimaryKey(String tableName);

  /**
   * Return java type for SQL type.
   * @param sqlType     sql type
   * @return            java type
   */
  public abstract String toJavaType(int sqlType);

    /**
     * Return hive type for SQL type.
     * @param sqlType   sql type
     * @return          hive type
     */
  public abstract String toHiveType(int sqlType);

  /**
   * Return an unordered mapping from colname to sqltype for
   * all columns in a table.
   *
   * The Integer type id is a constant from java.sql.Types
   */
  public abstract Map<String, Integer> getColumnTypes(String tableName);

  /**
   * This method allows various connection managers to indicate if they support
   * staging data for export jobs. The managers that do support this must
   * override this method and return <tt>true</tt>.
   *
   * @return true if the connection manager supports staging data for export
   * use-case.
   */
  public boolean supportsStagingForExport() {
    return false;
  }

  /**
   * Returns the count of all rows that exist in the given table.
   * @param tableName the name of the table which will be queried.
   * @return the number of rows present in the given table.
   * @throws SQLException if an error occurs during execution
   * @throws UnsupportedOperationException if the connection manager does not
   * support this operation.
   */
  public long getTableRowCount(String tableName) throws SQLException {
    throw new UnsupportedOperationException();
  }

  /**
   * Deletes all records from the given table. This method is invoked during
   * and export run when a staging table is specified. The staging table is
   * cleaned before the commencement of export job, and after the data has
   * been moved to the target table.
   * @param tableName name of the table which will be emptied.
   * @throws SQLException if an error occurs during execution
   * @throws UnsupportedOperationException if the connection manager does not
   * support this operation.
   */
  public void deleteAllRecords(String tableName) throws SQLException {
    throw new UnsupportedOperationException();
  }

  /**
   * Migrates all records from the given <tt>fromTable</tt> to the target
   * <tt>toTable</tt>. This method is invoked as a last step of an export
   * run where the staging is used to collect data before pushing it into the
   * target table.
   * @param fromTable the name of the staging table
   * @param toTable the name of the target table
   * @throws SQLException if an error occurs during execution
   * @throws UnsupportedOperationException if the connection manager does not
   * support this operation.
   */
  public void migrateData(String fromTable, String toTable)
    throws SQLException {
      throw new UnsupportedOperationException();
  }

  /**
   * Return an unordered mapping from colname to sqltype for
   * all columns in a query.
   *
   * The Integer type id is a constant from java.sql.Types
   */
  public Map<String, Integer> getColumnTypesForQuery(String query) {
    LOG.error("This database does not support free-form query column types.");
    return null;
  }

  /**
   * Execute a SQL statement to read the named set of columns from a table.
   * If columns is null, all columns from the table are read. This is a direct
   * (non-parallelized) read of the table back to the current client.
   * The client is responsible for calling ResultSet.close() when done with the
   * returned ResultSet object, and for calling release() after that to free
   * internal state.
   */
  public abstract ResultSet readTable(String tableName, String [] columns)
      throws SQLException;

  /**
   * @return the actual database connection.
   */
  public abstract Connection getConnection() throws SQLException;

  /**
   * @return a string identifying the driver class to load for this
   * JDBC connection type.
   */
  public abstract String getDriverClass();

  /**
   * Execute a SQL statement 's' and print its results to stdout.
   */
  public abstract void execAndPrint(String s);

  /**
   * Perform an import of a table from the database into HDFS.
   */
  public abstract void importTable(ImportJobContext context)
      throws IOException, ImportException;

  /**
   * Perform an import of a free-form query from the database into HDFS.
   */
  public void importQuery(ImportJobContext context)
      throws IOException, ImportException {
    throw new ImportException(
        "This database only supports table-based imports.");
  }

  /**
   * When using a column name in a generated SQL query, how (if at all)
   * should we escape that column name? e.g., a column named "table"
   * may need to be quoted with backtiks: "`table`".
   *
   * @param colName the column name as provided by the user, etc.
   * @return how the column name should be rendered in the sql text.
   */
  public String escapeColName(String colName) {
    return colName;
  }

  /**
   * When using a table name in a generated SQL query, how (if at all)
   * should we escape that column name? e.g., a table named "table"
   * may need to be quoted with backtiks: "`table`".
   *
   * @param tableName the table name as provided by the user, etc.
   * @return how the table name should be rendered in the sql text.
   */
  public String escapeTableName(String tableName) {
    return tableName;
  }

  /**
   * Perform any shutdown operations on the connection.
   */
  public abstract void close() throws SQLException;

  /**
   * Export data stored in HDFS into a table in a database.
   * This inserts new rows into the target table.
   */
  public void exportTable(ExportJobContext context)
      throws IOException, ExportException {
    throw new ExportException("This database does not support exports");
  }

  /**
   * Export updated data stored in HDFS into a database table.
   * This updates existing rows in the target table, based on the
   * updateKeyCol specified in the context's SqoopOptions.
   */
  public void updateTable(ExportJobContext context)
      throws IOException, ExportException {
    throw new ExportException("This database does not support updates");
  }

  /**
   * If a method of this ConnManager has returned a ResultSet to you,
   * you are responsible for calling release() after you close the
   * ResultSet object, to free internal resources. ConnManager
   * implementations do not guarantee the ability to have multiple
   * returned ResultSets available concurrently. Requesting a new
   * ResultSet from a ConnManager may cause other open ResulSets
   * to close.
   */
  public abstract void release();

  /**
   * Return the current time from the perspective of the database server.
   * Return null if this cannot be accessed.
   */
  public Timestamp getCurrentDbTimestamp() {
    LOG.warn("getCurrentDbTimestamp(): Using local system timestamp.");
    return new Timestamp(System.currentTimeMillis());
  }

  /**
   * Given a non-null Timestamp, return the quoted string that can
   * be inserted into a SQL statement, representing that timestamp.
   */
  public String timestampToQueryString(Timestamp ts) {
    return "'" + ts + "'";
  }

  /**
   * This method allows the ConnManager to override the creation of an
   * input-bounds query that is used to create splits when running import
   * based on free-form query. Any non-null return value is used, whereas a null
   * return value indicates that the default input bounds query should be
   * used.
   * @param splitByCol the column name to split on.
   * @param sanitizedQuery the sanitized input query specified by user.
   * @return an input-bounds query or <tt>null</tt> if default query is
   * acceptable.
   */
  public String getInputBoundsQuery(String splitByCol, String sanitizedQuery) {
      return null;
  }
}

