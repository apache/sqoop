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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.db.SQLFailureHandler;

/**
 * Represents a database update thread that runs asynchronously to perform
 * database operations on the given records
 *
 * The asynchronous thread receives a batch of records that it writes to
 * the database. It uses the configured connection handler to recover from
 * connection failures (if possible) until the records are inserted/updated
 * in the database
 */
public abstract class SQLServerAsyncDBExecThread extends Thread {

  private static final Log LOG = LogFactory.getLog(
     SQLServerAsyncDBExecThread.class);

  // Recover failed operations for RETRY_MAX
  protected static final int RETRY_MAX = 3;
  protected static final int RETRY_INTERVAL = 5 * 1000;

  private Connection conn; // The connection to the database.
  private DBConfiguration dbConf = null;

  private SynchronousQueue<List<SqoopRecord>> recordListQueue;
  private boolean stop = false;
  private Exception err;

  // The SQL handler to be used for recovering failed write operations
  private SQLFailureHandler failureHandler = null;

  protected Configuration conf = null;
  protected String tableName;
  protected String [] columnNames; // The columns to insert into.
  protected int columnCount; // If columnNames is null, tells ## of cols.

  /**
   * Create a new thread that interacts with the database.
   */
  public SQLServerAsyncDBExecThread() {
    recordListQueue = new SynchronousQueue<List<SqoopRecord>>();
  }

  /**
   * Initialize the writer thread with Job Configuration.
   */
  public void initialize(Configuration c) throws IOException {
    // Create a DBConf from the given Configuration
    this.conf = c;
    this.dbConf = new DBConfiguration(conf);

    tableName = dbConf.getOutputTableName();
    columnNames = dbConf.getOutputFieldNames();
    columnCount = dbConf.getOutputFieldCount();

    // Get the SQL Failure handler to be used for recovering failed write
    // operations
    failureHandler = getSQLFailureHandler();
    failureHandler.initialize(conf);
  }

  /**
   * Get the SQL Failure handler to be used for recovering failed write
   * operations.
   */
  protected SQLFailureHandler getSQLFailureHandler() throws IOException {
    if (failureHandler == null) {
      Class<? extends SQLFailureHandler> connHandlerClass;
      try {
        String className = conf.get(
          SQLServerResilientExportOutputFormat.EXPORT_FAILURE_HANDLER_CLASS);
        // Get the class-name set in configuration
        connHandlerClass =
          (Class<? extends SQLFailureHandler>) conf.getClassByName(className);
      } catch (ClassNotFoundException ex) {
        LOG.error("Failed to find class: "
          + SQLServerResilientExportOutputFormat.EXPORT_FAILURE_HANDLER_CLASS);
        throw new IOException(ex);
      }

      // Verify handler class is a subclass of SQLFailureHandler
      if (!SQLFailureHandler.class.isAssignableFrom(connHandlerClass)) {
        String error = "A subclass of " + SQLFailureHandler.class.getName()
          + " is expected. Actual class set is: "
          + connHandlerClass.getName();
        LOG.error(error);
        throw new IOException(error);
      }
      LOG.trace("Using connection handler class: " + connHandlerClass);
      // Load the configured connection failure handler
      failureHandler = ReflectionUtils.newInstance(connHandlerClass, conf);
    }
    return failureHandler;
  }

  protected DBConfiguration getDBConfiguration() {
    return dbConf;
  }

  /**
   * Create the connection to use for exporting records. If the connection is
   * already created, then return it
   */
  protected Connection getConnection() throws SQLException {
    if (conn == null || conn.isClosed()) {
      try {
        conn = dbConf.getConnection();
        configureConnection();
      } catch (ClassNotFoundException cnfEx) {
        LOG.error("Cannot create connection. Driver class not found: "
          + cnfEx);
      }
    }
    return conn;
  }

  protected Configuration getConf() {
    return this.conf;
  }

  /**
   * Configure the connection object used for writing records to the database.
   * Subclasses should override this method to change connection
   * configuration.
   */
  protected void configureConnection() throws SQLException {
    conn.setAutoCommit(false);
  }

  /**
   * Enqueue the next list of records to be processed, if the previous
   * list is still being processed, we will block until it completes.
   * The call blocks if another batch of records is still being processed.
   */
  public void put(List<SqoopRecord> recordList)
      throws InterruptedException, IOException {
    // Check for any exception raised when writing to the database
    Exception lastException = getLastError();
    if (lastException != null) {
      LOG.error("Asynchronous writer thread encountered the following "
        + "exception: " + lastException.toString());
      throw new IOException(lastException);
    }

    recordListQueue.put((List<SqoopRecord>) recordList);
  }

  /**
   * Get the next list of records to be processed, or wait until one becomes
   * available.
   * @throws InterruptedException
   */
  protected List<SqoopRecord> take() throws InterruptedException {
    return recordListQueue.take();
  }

  /** {@inheritDoc} */
  @Override
  public void start() {
    stop = false;
    super.start();
  }

  /**
   * Stop the current thread skipping any subsequent database operations on
   * records that have not yet been processed.
   */
  public void close() {
    stop = true;
    // In case the thread is blocked inside the take() method, offer
    // an empty list which is simply ignored
    recordListQueue.offer(new ArrayList<SqoopRecord>());
  }

  /**
   * Indicate whether the current thread is running and accepting records to
   * send to the database.
   */
  public boolean isRunning() {
    return !stop;
  }

  /**
   * Consume records from the list to be written to the database.
   * Block until we have records available in the list.
   */
  @Override
  public void run() {
    while (!stop) {
      List<SqoopRecord> recordList = null;
      try {
        // Block until we get a list of records to process
        recordList = take();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted while waiting for more records");
        continue;
      }

      // Ensure we do not have a null or empty list
      if (recordList == null || recordList.size() == 0) {
        LOG.warn("Got a Null or empty list. skipping");
        continue;
      }

      // Write the current list of records to the database
      try {
        write(recordList);
      } catch (Exception ex) {
        LOG.error("Failed to write records.", ex);
        setLastError(ex);

        // Stop processing incoming batches and remove any queued ones
        close();
        recordListQueue.poll();
      }
    }
  }

  /**
   * Write the records to the database. If a failure occurs, it tries to
   * use the configured handler to recover from the failure, otherwise
   * a SQLException is throw
   */
  protected void write(List<SqoopRecord> records)
      throws SQLException, IOException {
    PreparedStatement stmt = null;
    int retryCount = RETRY_MAX;
    boolean doRetry = true;

    do {
      try {
        // Establish the connection to be used if not yet created
        getConnection();

        // Get the prepared statement to use for writing the records
        stmt = getPreparedStatement(records);

        // Execute the prepared statement
        executeStatement(stmt, records);

        // Statement executed successfully, no need to retry
        doRetry = false;
      } catch (SQLException sqlEx) {
        LOG.warn("Trying to recover from DB write failure: ", sqlEx);

        // Use configured connection handler to recover from the connection
        // failure and use the recovered connection.
        // If the failure cannot be recovered, an exception is thrown
        if (failureHandler.canHandleFailure(sqlEx)) {
          // Recover from connection failure
          this.conn = failureHandler.recover();

          // Configure the new connection before using it
          configureConnection();

          --retryCount;
          doRetry = (retryCount >= 0);
        } else {
          // Cannot recover using configured handler, re-throw
          throw new IOException("Registered handler cannot recover error "
            + "with SQL State: " + sqlEx.getSQLState() + ", error code: "
            + sqlEx.getErrorCode(), sqlEx);
        }
      }
    } while (doRetry);

    // Throw an exception if all retry attempts are consumed
    if (retryCount < 0) {
      throw new IOException("Failed to write to database after "
        + RETRY_MAX + " retries.");
    }
  }

  /**
   * Generate the PreparedStatement object that will be used to write records
   * to the database. All parameterized fields of the PreparedStatement must
   * be set in this method as well; this is usually based on the records
   * collected from the user in the records list
   *
   * This method must be overridden by sub-classes to define the database
   * operation to be executed for user records
   */
  protected abstract PreparedStatement getPreparedStatement(
      List<SqoopRecord> records) throws SQLException;

  /**
   * Execute the provided PreparedStatement, by default this assume batch
   * execute, but this can be overridden by subclasses for a different mode
   * of execution which should match getPreparedStatement implementation.
   */
  protected abstract void executeStatement(PreparedStatement stmt,
      List<SqoopRecord> records) throws SQLException;

  /**
   * Report any SQL Exception that could not be automatically handled or
   * recovered.
   *
   * If the error slot was already filled, then subsequent errors are
   * squashed until the user calls this method (which clears the error
   * slot).
   * @return any unrecovered SQLException that occurred due to a
   * previously-run database operation.
   */
  public synchronized Exception getLastError() {
    Exception e = this.err;
    this.err = null;
    return e;
  }

  private synchronized void setLastError(Exception e) {
    if (this.err == null) {
      // Just set it.
      LOG.error("Got exception in update thread: "
          + StringUtils.stringifyException(e));
      this.err = e;
    } else {
      // Slot is full. Log it and discard.
      LOG.error("Exception in update thread but error slot full: "
          + StringUtils.stringifyException(e));
    }
  }
}
