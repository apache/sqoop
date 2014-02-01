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
package org.apache.sqoop.mapreduce.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

import  org.apache.sqoop.lib.SqoopRecord;

/**
 * A RecordReader that reads records from a SQL table.
 * This record reader handles connection failures using the configured
 * connection failure handler
 */
public class SQLServerDBRecordReader<T extends SqoopRecord> extends
      SqlServerRecordReader<T> {

  private static final Log LOG =
      LogFactory.getLog(SQLServerDBRecordReader.class);

  // The SQL handler to be used for recovering failed read operations
  protected SQLFailureHandler failureHandler = null;

  // Recover failed reads for RETRY_MAX
  protected static final int RETRY_MAX = 3;

  // Name of the split column used to re-generate selectQueries after
  // connection failures
  private String splitColumn;
  private String lastRecordKey;

  public SQLServerDBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn,
      DBConfiguration dbConfig, String cond, String [] fields, String table,
      String dbProduct) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
  }

  @Override
  /** {@inheritDoc} */
  public T getCurrentValue() {
    T val = super.getCurrentValue();
    // Lookup the key of the last read record to use for recovering
    // As documented, the map may not be null, though it may be empty.
    Object lastRecordSplitCol = val.getFieldMap().get(splitColumn);
    lastRecordKey = (lastRecordSplitCol == null) ? null
        : lastRecordSplitCol.toString();
    return val;
  }

  /**
   * Load the SQLFailureHandler configured for use by the record reader.
   */
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // Load the configured connection failure handler
    Configuration conf = getConf();
    if (conf == null) {
      LOG.error("Configuration cannot be NULL");
    }

    Class connHandlerClass;
    try {
      String className = conf.get(
        SQLServerDBInputFormat.IMPORT_FAILURE_HANDLER_CLASS);

      // Get the class-name set in configuration
      connHandlerClass = conf.getClassByName(className);
    } catch (ClassNotFoundException ex) {
      LOG.error("Failed to find class: "
        + SQLServerDBInputFormat.IMPORT_FAILURE_HANDLER_CLASS);
      throw new IOException(ex);
    }

    // Verify handler class is a subclass of SQLFailureHandler
    if (!SQLFailureHandler.class.isAssignableFrom(connHandlerClass)) {
      String error = "A subclass of " + SQLFailureHandler.class.getName()
        + " is expected. Actual class set is: " + connHandlerClass.getName();
      LOG.error(error);
      throw new IOException(error);
    }
    LOG.trace("Using connection handler class: " + connHandlerClass);

    // Load the configured connection failure handler
    failureHandler = ReflectionUtils.newInstance(
      (Class<? extends SQLFailureHandler>)connHandlerClass, conf);

    // Initialize the connection handler with using job configuration
    failureHandler.initialize(conf);

    // Get the split-by column
    splitColumn = getDBConf().getInputOrderBy();
    if (splitColumn == null || splitColumn.length() == 0) {
      throw new IOException("Split column must be set");
    }

    // Ensure the split-column is not escaped so that we can use it to search
    // in the record map
    int splitColLen = splitColumn.length();
    if (splitColLen > 2 && splitColumn.charAt(0) == '['
      && splitColumn.charAt(splitColLen-1) == ']') {
      splitColumn = splitColumn.substring(1, splitColLen - 1);
    }
  }

  @Override
  /**
   * Read the next key, value pair.
   * Try to recover failed connections using the configured connection failure
   * handler before retrying the failed operation
   */
  public boolean nextKeyValue() throws IOException {
    boolean valueReceived = false;
    int retryCount = RETRY_MAX;
    boolean doRetry = true;

    do {
      try {
        // Try to get the next key/value pairs
        valueReceived = super.nextKeyValue();
        doRetry = false;
      } catch (IOException ioEx) {
        LOG.warn("Trying to recover from DB read failure: ", ioEx);
        Throwable cause = ioEx.getCause();

        // Use configured connection handler to recover from the connection
        // failure and use the newly constructed connection.
        // If the failure cannot be recovered, an exception is thrown
        if (failureHandler.canHandleFailure(cause)) {
          // Recover from connection failure
          Connection conn = failureHandler.recover();

          // Configure the new connection before using it
          configureConnection(conn);
          setConnection(conn);

          --retryCount;
          doRetry = (retryCount >= 0);
        } else {
          // Cannot recovered using configured handler, re-throw
          throw new IOException("Cannection handler cannot recover failure: ",
              ioEx);
        }
      }
    } while (doRetry);

    // Rethrow the exception if all retry attempts are consumed
    if (retryCount < 0) {
      throw new IOException("Failed to read from database after "
        + RETRY_MAX + " retries.");
    }

    return valueReceived;
  }

  /**
   * Configure the provided Connection for record reads.
   */
  protected void configureConnection(Connection conn) throws IOException {
    try {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    } catch (SQLException sqlEx) {
      LOG.error("Failed to configure SQL Connection");
      throw new IOException(sqlEx);
    }
  }

  /** Returns the query for selecting the records,
   * For handling connection recovery we always want to start from the last
   * record that was successfully read.
   */
  protected String getSelectQuery() {
    // Last seen record key is only expected to be unavailable if no reads
    // ever happened
    String selectQuery;
    if (lastRecordKey == null) {
      selectQuery = super.getSelectQuery();
    } else {
      // If last record key is available, construct the select query to start
      // from
      DataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit =
          (DataDrivenDBInputFormat.DataDrivenDBInputSplit) getSplit();
      StringBuilder lowerClause = new StringBuilder();
      lowerClause.append(getDBConf().getInputOrderBy());
      lowerClause.append(" > ");
      lowerClause.append(lastRecordKey.toString());

      // Get the select query with the lowerClause, and split upper clause
      selectQuery = getSelectQuery(lowerClause.toString(),
          dataSplit.getUpperClause());
    }

    return selectQuery;
  }
}
