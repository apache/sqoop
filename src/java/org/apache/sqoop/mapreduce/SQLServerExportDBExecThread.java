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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.manager.SQLServerManager;

/**
 * Represents a database update thread that runs asynchronously to perform
 * database insert operations on the given records
 *
 * The asynchronous thread receives a batch of records that it writes to
 * the database. It uses the configured connection handler to recover from
 * connection failures (if possible) until the records are inserted/updated
 * in the database
 */
public class SQLServerExportDBExecThread extends
    SQLServerAsyncDBExecThread {

  private static final Log LOG = LogFactory.getLog(
     SQLServerExportDBExecThread.class);

  protected static final String SQLSTATE_CODE_CONSTRAINT_VIOLATION = "23000";
  private boolean failedCommit = false;

  /**
   * Generate the PreparedStatement object that will be used to insert records
   * to the database. All parameterized fields of the PreparedStatement must
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

    // Create a PreparedStatement object to insert all records
    stmt = conn.prepareStatement(getInsertStatement(records.size()));

    // Inject the record parameters into the VALUES clauses.
    for (SqoopRecord record : records) {
      record.write(stmt, 0);
      stmt.addBatch();
    }

    return stmt;
  }

  /**
   * Execute the provided PreparedStatement, by default this assume batch
   * execute, but this can be overridden by subclasses for a different mode
   * of execution which should match getPreparedStatement implementation
   */
  @Override
  protected void executeStatement(PreparedStatement stmt,
      List<SqoopRecord> records) throws SQLException {
    // On failures in commit step, we cannot guarantee that transactions have
    // been successfully committed to the database.
    // This can result in access violation issues for columns with unique
    // constraints.
    // One way to handle this is check whether records are committed in the
    // database before propagating the failure for retry. However in case we
    // have connection loss, we wont be able to access the database.
    // An alternative option is to ignore violation issues in the next retry
    // in case the records have been already committed

    Connection conn = getConnection();
    try {
     stmt.executeBatch();
    } catch (SQLException execSqlEx) {
      LOG.warn("Error executing statement: " + execSqlEx);
      if (failedCommit &&
        canIgnoreForFailedCommit(execSqlEx.getSQLState())){
        LOG.info("Ignoring error after failed commit");
      } else {
        throw execSqlEx;
      }
    }

    // If the batch of records is executed successfully, then commit before
    // processing the next batch of records
    try {
      conn.commit();
      failedCommit = false;
    } catch (SQLException commitSqlEx) {
      LOG.warn("Error while committing transactions: " + commitSqlEx);

      failedCommit = true;
      throw commitSqlEx;
    }
  }

  /**
   * Create an INSERT statement for the given records
   */
  protected String getInsertStatement(int numRows) {
    StringBuilder sb = new StringBuilder();

    if (getConf().getBoolean(SQLServerManager.IDENTITY_INSERT_PROP, false)) {
      LOG.info("Enabling identity inserts");
      sb.append("SET IDENTITY_INSERT ").append(tableName).append(" ON ");
    }

    sb.append("INSERT INTO " + tableName + " ");

    String tableHints = getConf().get(SQLServerManager.TABLE_HINTS_PROP);
    if (tableHints != null) {
      LOG.info("Using table hints: " + tableHints);
      sb.append(" WITH (").append(tableHints).append(") ");
    }

    int numSlots;
    if (this.columnNames != null) {
      numSlots = this.columnNames.length;

      sb.append("(");
      boolean first = true;
      for (String col : columnNames) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(col);
        first = false;
      }

      sb.append(") ");
    } else {
      numSlots = this.columnCount; // set if columnNames is null.
    }

    sb.append("VALUES ");

    // generates the (?, ?, ?...).
    sb.append("(");
    for (int i = 0; i < numSlots; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append("?");
    }
    sb.append(")");

    return sb.toString();
  }

  /**
   * Specify whether the given SQL State error is expected after failed
   * commits. For example, constraint violation
   */
  protected boolean canIgnoreForFailedCommit(String sqlState){
    return sqlState == SQLSTATE_CODE_CONSTRAINT_VIOLATION;
  }
}
