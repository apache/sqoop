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

package com.cloudera.sqoop.mapreduce;

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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * Insert the emitted keys as records into a database table.
 * This supports a configurable "spill threshold" at which
 * point intermediate transactions are committed. 
 *
 * Record objects are buffered before actually performing the INSERT
 * statements; this requires that the key implement the
 * SqoopRecord interface.
 *
 * Uses DBOutputFormat/DBConfiguration for configuring the output.
 */
public class ExportOutputFormat<K extends SqoopRecord, V> 
    extends OutputFormat<K, V> {

  /** conf key: number of rows to export per INSERT statement. */
  public static final String RECORDS_PER_STATEMENT_KEY =
      "sqoop.export.records.per.statement";

  /** conf key: number of INSERT statements to bundle per tx.
   * If this is set to -1, then a single transaction will be used
   * per task. Note that each statement may encompass multiple
   * rows, depending on the value of sqoop.export.records.per.statement.
   */
  public static final String STATEMENTS_PER_TRANSACTION_KEY =
      "sqoop.export.statements.per.transaction";

  private static final int DEFAULT_RECORDS_PER_STATEMENT = 100;
  private static final int DEFAULT_STATEMENTS_PER_TRANSACTION = 100;
  private static final int UNLIMITED_STATEMENTS_PER_TRANSACTION = -1;

  private static final Log LOG = LogFactory.getLog(ExportOutputFormat.class);

  @Override
  /** {@inheritDoc} */
  public void checkOutputSpecs(JobContext context) 
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    DBConfiguration dbConf = new DBConfiguration(conf);

    // Sanity check all the configuration values we need.
    if (null == conf.get(DBConfiguration.URL_PROPERTY)) {
      throw new IOException("Database connection URL is not set.");
    } else if (null == dbConf.getOutputTableName()) {
      throw new IOException("Table name is not set for export");
    } else if (null == dbConf.getOutputFieldNames()
        && 0 == dbConf.getOutputFieldCount()) {
      throw new IOException(
          "Output field names are null and zero output field count set.");
    }
  }

  @Override
  /** {@inheritDoc} */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    return new OutputCommitter() {
      public void abortTask(TaskAttemptContext taskContext) { }
      public void cleanupJob(JobContext jobContext) { }
      public void commitTask(TaskAttemptContext taskContext) { }
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }
      public void setupJob(JobContext jobContext) { }
      public void setupTask(TaskAttemptContext taskContext) { }
    };
  }

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
      throws IOException {
    try {
      return new ExportRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Represents a database update operation that should be performed
   * by an asynchronous background thread.
   * AsyncDBOperation objects are immutable.
   * They MAY contain a statement which should be executed. The
   * statement may also be null.
   *
   * They may also set 'forceCommit' to true. If true, then the
   * executor of this operation should commit the current
   * transaction, even if stmt is null.
   */
  private static class AsyncDBOperation {
    private final PreparedStatement stmt;
    private final boolean forceCommit;
    private final boolean close;

    /**
     * Create an asynchronous database operation.
     * @param s the statement, if any, to execute.
     * @param forceCommit if true, the current transaction should be committed.
     * @param close if true, the executor thread should stop after processing
     * this operation.
     */
    public AsyncDBOperation(PreparedStatement s, boolean forceCommit,
        boolean close) {
      this.stmt = s;
      this.forceCommit = forceCommit;
      this.close = close;
    }

    /**
     * @return a statement to run as an update.
     */
    public PreparedStatement getStatement() {
      return stmt;
    }

    /**
     * @return true if the executor should commit the current transaction.
     * If getStatement() is non-null, the statement is run first.
     */
    public boolean requiresCommit() {
      return forceCommit;
    }

    /**
     * @return true if the executor should stop after this command.
     */
    public boolean stop() {
      return this.close;
    }
  }

  /**
   * A thread that runs the database interactions asynchronously
   * from the OutputCollector.
   */
  private static class ExportUpdateThread extends Thread {

    private final Connection conn; // The connection to the database.
    private SQLException err; // Error from a previously-run statement.

    // How we receive database operations from the RecordWriter.
    private SynchronousQueue<AsyncDBOperation> opsQueue;

    protected int curNumStatements; // statements executed thus far in the tx.
    protected final int stmtsPerTx;  // statements per transaction.

    /**
     * Create a new update thread that interacts with the database.
     * @param conn the connection to use. This must only be used by this
     * thread.
     * @param stmtsPerTx the number of statements to execute before committing
     * the current transaction.
     */
    public ExportUpdateThread(Connection conn, int stmtsPerTx) {
      this.conn = conn;
      this.err = null;
      this.opsQueue = new SynchronousQueue<AsyncDBOperation>();
      this.stmtsPerTx = stmtsPerTx;
    }

    public void run() {
      while (true) {
        AsyncDBOperation op = null;
        try {
          op = opsQueue.take();
        } catch (InterruptedException ie) {
          LOG.warn("Interrupted retrieving from operation queue: "
              + StringUtils.stringifyException(ie));
          continue;
        }

        if (null == op) {
          // This shouldn't be allowed to happen.
          LOG.warn("Null operation in queue; illegal state.");
          continue;
        }

        PreparedStatement stmt = op.getStatement();
        // Synchronize on the connection to ensure it does not conflict
        // with the prepareStatement() call in the main thread.
        synchronized (conn) {
          try {
            if (null != stmt) {
              stmt.executeUpdate();
              stmt.close();
              stmt = null;
              this.curNumStatements++;
            }

            if (op.requiresCommit() || (curNumStatements >= stmtsPerTx
                && stmtsPerTx != UNLIMITED_STATEMENTS_PER_TRANSACTION)) {
              LOG.debug("Committing transaction of " + curNumStatements
                  + " statements");
              this.conn.commit();
              this.curNumStatements = 0;
            }
          } catch (SQLException sqlE) {
            setLastError(sqlE);
          } finally {
            // Close the statement on our way out if that didn't happen
            // via the normal execution path.
            if (null != stmt) {
              try {
                stmt.close();
              } catch (SQLException sqlE) {
                setLastError(sqlE);
              }
            }

            // Always check whether we should end the loop, regardless
            // of the presence of an exception.
            if (op.stop()) {
              // Don't continue processing after this operation.
              try {
                conn.close();
              } catch (SQLException sqlE) {
                setLastError(sqlE);
              }
              return;
            }
          } // try .. catch .. finally.
        } // synchronized (conn)
      }
    }

    /**
     * Allows a user to enqueue the next database operation to run.
     * Since the connection can only execute a single operation at a time,
     * the put() method may block if another operation is already underway.
     * @param op the database operation to perform.
     */
    public void put(AsyncDBOperation op) throws InterruptedException {
      opsQueue.put(op);
    }

    /**
     * If a previously-executed statement resulted in an error, post it here.
     * If the error slot was already filled, then subsequent errors are
     * squashed until the user calls this method (which clears the error
     * slot).
     * @return any SQLException that occurred due to a previously-run
     * statement.
     */
    public synchronized SQLException getLastError() {
      SQLException e = this.err;
      this.err = null;
      return e;
    }

    private synchronized void setLastError(SQLException e) {
      if (this.err == null) {
        // Just set it.
        LOG.error("Got exception in update thread: "
            + StringUtils.stringifyException(e));
        this.err = e;
      } else {
        // Slot is full. Log it and discard.
        LOG.error("SQLException in update thread but error slot full: "
            + StringUtils.stringifyException(e));
      }
    }
  }

  /**
   * RecordWriter to write the output to a row in a database table.
   * The actual database updates are executed in a second thread.
   */
  public class ExportRecordWriter extends RecordWriter<K, V> {

    protected Connection connection;

    protected Configuration conf;

    protected int rowsPerStmt; // rows to insert per statement.
    
    // Buffer for records to be put in an INSERT statement.
    protected List<SqoopRecord> records;

    protected String tableName;
    protected String [] columnNames; // The columns to insert into.
    protected int columnCount; // If columnNames is null, tells ## of cols.

    // Background thread to actually perform the updates.
    private ExportUpdateThread updateThread;
    private boolean startedUpdateThread;

    public ExportRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      this.conf = context.getConfiguration();

      this.rowsPerStmt = conf.getInt(RECORDS_PER_STATEMENT_KEY,
          DEFAULT_RECORDS_PER_STATEMENT);
      int stmtsPerTx = conf.getInt(STATEMENTS_PER_TRANSACTION_KEY,
          DEFAULT_STATEMENTS_PER_TRANSACTION);

      DBConfiguration dbConf = new DBConfiguration(conf);
      this.connection = dbConf.getConnection();
      this.tableName = dbConf.getOutputTableName();
      this.columnNames = dbConf.getOutputFieldNames();
      this.columnCount = dbConf.getOutputFieldCount();

      this.connection.setAutoCommit(false);

      this.records = new ArrayList<SqoopRecord>(this.rowsPerStmt);

      this.updateThread = new ExportUpdateThread(connection, stmtsPerTx);
      this.updateThread.setDaemon(true);
      this.startedUpdateThread = false;
    }

    /**
     * @return an INSERT statement suitable for inserting 'numRows' rows.
     */
    protected String getInsertStatement(int numRows) {
      StringBuilder sb = new StringBuilder();

      sb.append("INSERT INTO " + tableName + " ");

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

      // generates the (?, ?, ?...) used for each row.
      StringBuilder sbRow = new StringBuilder();
      sbRow.append("(");
      for (int i = 0; i < numSlots; i++) {
        if (i != 0) {
          sbRow.append(", ");
        }

        sbRow.append("?");
      }
      sbRow.append(")");

      // Now append that numRows times.
      for (int i = 0; i < numRows; i++) {
        if (i != 0) {
          sb.append(", ");
        }

        sb.append(sbRow);
      }

      return sb.toString();
    }

    /**
     * Takes the current contents of 'records' and formats and executes the
     * INSERT statement.
     * @param closeConn if true, commits the transaction and closes the
     * connection.
     */
    private void insertRows(boolean closeConn)
        throws InterruptedException, SQLException {

      if (!startedUpdateThread) {
        this.updateThread.start();
        this.startedUpdateThread = true;
      }

      PreparedStatement stmt = null;
      boolean successfulPut = false;
      try {
        if (records.size() > 0) {
          // Synchronize on connection to ensure this does not conflict
          // with the operations in the update thread.
          synchronized (connection) {
            stmt = connection.prepareStatement(
              getInsertStatement(records.size()));
          }

          // Inject the record parameters into the VALUES clauses.
          int position = 0;
          for (SqoopRecord record : records) {
            position += record.write(stmt, position);
          }

          this.records.clear();
        }

        // Pass this operation off to the update thread. This will block if
        // the update thread is already performing an update.
        AsyncDBOperation op = new AsyncDBOperation(stmt, closeConn, closeConn);
        updateThread.put(op);
        successfulPut = true; // op has been posted to the other thread.
      } finally {
        if (!successfulPut && null != stmt) {
          // We created a statement but failed to enqueue it. Close it.
          stmt.close();
        }
      }

      // Check for any previous SQLException. If one happened, rethrow it here.
      SQLException lastException = updateThread.getLastError();
      if (null != lastException) {
        throw lastException;
      }
    }

    @Override
    /** {@inheritDoc} */
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      try {
        insertRows(true);
      } catch (SQLException sqle) {
        throw new IOException(sqle);
      } finally {
        updateThread.join();
      }

      // If we're not leaving on an error return path already,
      // now that updateThread is definitely stopped, check that the
      // error slot remains empty.
      SQLException lastErr = updateThread.getLastError();
      if (null != lastErr) {
        throw new IOException(lastErr);
      }
    }

    @Override
    /** {@inheritDoc} */
    public void write(K key, V value)
        throws InterruptedException, IOException {
      try {
        records.add((SqoopRecord) key.clone());
        if (records.size() >= this.rowsPerStmt) {
          insertRows(false);
        }
      } catch (CloneNotSupportedException cnse) {
        throw new IOException("Could not buffer record", cnse);
      } catch (SQLException sqlException) {
        throw new IOException(sqlException);
      }
    }
  }
}
