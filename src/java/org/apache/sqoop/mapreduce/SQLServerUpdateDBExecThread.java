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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.lib.SqoopRecord;

/**
 * Represents a database update thread that runs asynchronously to perform
 * database Update operations on the given records
 *
 * The asynchronous thread receives a batch of records that it writes to
 * the database. It uses the configured connection handler to recover from
 * connection failures (if possible) until the records are inserted/updated
 * in the database
 */
public class SQLServerUpdateDBExecThread extends
    SQLServerExportDBExecThread {

  private static final Log LOG = LogFactory.getLog(
     SQLServerUpdateDBExecThread.class);

  protected String [] updateCols; // The columns containing the fixed key.

  /**
   * Initialize the writer thread with Job Configuration
   */
  @Override
  public void initialize(Configuration conf) throws IOException {
    super.initialize(conf);

    // Get the update columns
    String updateKeyColumns =
      conf.get(ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY);

    Set<String> updateKeys = new LinkedHashSet<String>();
    StringTokenizer stok = new StringTokenizer(updateKeyColumns, ",");
    while (stok.hasMoreTokens()) {
      String nextUpdateKey = stok.nextToken().trim();
      if (nextUpdateKey.length() > 0) {
        updateKeys.add(nextUpdateKey);
      } else {
        throw new RuntimeException("Invalid update key column value specified"
            + ": '" + updateKeyColumns + "'");
      }
    }

    updateCols = updateKeys.toArray(new String[updateKeys.size()]);
  }

  /**
   * Generate the PreparedStatement object that will be used to update records
   * in the database. All parameterized fields of the PreparedStatement must
   * be set in this method as well; this is usually based on the records
   * collected from the user in the records list
   *
   * This method must be overridden by sub-classes to define the database
   * operation to be executed for user records
   */
  @Override
  protected PreparedStatement getPreparedStatement(
      List<SqoopRecord> records) throws SQLException {
    PreparedStatement stmt = null;
    Connection conn = getConnection();

    // Create a PreparedStatement object to Update all records
    stmt = conn.prepareStatement(getUpdateStatement());

    // Inject the record parameters into the UPDATE and WHERE clauses.  This
    // assumes that the update key column is the last column serialized in
    // by the underlying record. Our code auto-gen process for exports was
    // responsible for taking care of this constraint.
    for (SqoopRecord record : records) {
      record.write(stmt, 0);
      stmt.addBatch();
    }

    return stmt;
  }

  /**
   * @return an UPDATE statement that modifies rows based on a single key
   * column (with the intent of modifying a single row).
   */
  protected String getUpdateStatement() {
    StringBuilder sb = new StringBuilder();
    sb.append("UPDATE " + this.tableName + " SET ");

    boolean first = true;
    for (String col : this.columnNames) {
      if (!first) {
        sb.append(", ");
      }

      sb.append(col);
      sb.append("=?");
      first = false;
    }

    sb.append(" WHERE ");
    first = true;
    for (int i = 0; i < updateCols.length; i++) {
      if (!first) {
        sb.append(" AND ");
      }
      sb.append(updateCols[i]).append("=?");
      first = false;
    }
    return sb.toString();
  }
}
