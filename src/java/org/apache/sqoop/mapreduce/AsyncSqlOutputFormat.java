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
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.SynchronousQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * Abstract OutputFormat class that allows the RecordWriter to buffer
 * up SQL commands which should be executed in a separate thread after
 * enough commands are created.
 *
 * This supports a configurable "spill threshold" at which
 * point intermediate transactions are committed.
 *
 * Uses DBOutputFormat/DBConfiguration for configuring the output.
 * This is used in conjunction with the abstract AsyncSqlRecordWriter
 * class.
 *
 * Clients of this OutputFormat must implement getRecordWriter(); the
 * returned RecordWriter is intended to subclass AsyncSqlRecordWriter.
 */
public abstract class AsyncSqlOutputFormat<K extends SqoopRecord, V>
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

  /**
   * Default number of records to put in an INSERT statement or
   * other batched update statement.
   */
  public static final int DEFAULT_RECORDS_PER_STATEMENT = 100;

  /**
   * Default number of statements to execute before committing the
   * current transaction.
   */
  public static final int DEFAULT_STATEMENTS_PER_TRANSACTION = 100;

  /**
   * Value for STATEMENTS_PER_TRANSACTION_KEY signifying that we should
   * not commit until the RecordWriter is being closed, regardless of
   * the number of statements we execute.
   */
  public static final int UNLIMITED_STATEMENTS_PER_TRANSACTION = -1;

  private static final Log LOG = LogFactory.getLog(AsyncSqlOutputFormat.class);

  @Override
  /** {@inheritDoc} */
  public void checkOutputSpecs(JobContext context)
      throws IOException, InterruptedException {
  }

  @Override
  /** {@inheritDoc} */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new NullOutputCommitter();
  }

  /**
   * Represents a database update operation that should be performed
   * by an asynchronous background thread.
   * AsyncDBOperation objects are immutable.
   * They MAY contain a statement which should be executed. The
   * statement may also be null.
   *
   * They may also set 'commitAndClose' to true. If true, then the
   * executor of this operation should commit the current
   * transaction, even if stmt is null, and then stop the executor
   * thread.
   */
  public static class AsyncDBOperation {
    private final PreparedStatement stmt;
    private final boolean isBatch;
    private final boolean commit;
    private final boolean stopThread;

    @Deprecated
    /** Do not use AsyncDBOperation(PreparedStatement s, boolean
     * commitAndClose, boolean batch). Use AsyncDBOperation(PreparedStatement
     *  s, boolean batch, boolean commit, boolean stopThread) instead.
     */
    public AsyncDBOperation(PreparedStatement s, boolean commitAndClose,
        boolean batch) {
        this(s, batch, commitAndClose, commitAndClose);
    }

    /**
     * Create an asynchronous database operation.
     * @param s the statement, if any, to execute.
     * @param batch is true if this is a batch PreparedStatement, or false
     * if it's a normal singleton statement.
     * @param commit is true if this statement should be committed to the
     * database.
     * @param stopThread if true, the executor thread should stop after this
     * operation.
     */
    public AsyncDBOperation(PreparedStatement s, boolean batch,
        boolean commit, boolean stopThread) {
      this.stmt = s;
      this.isBatch = batch;
      this.commit = commit;
      this.stopThread = stopThread;
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
      return this.commit;
    }

    /**
     * @return true if the executor should stop after this command.
     */
    public boolean stop() {
      return this.stopThread;
    }

    /**
     * @return true if this is a batch SQL statement.
     */
    public boolean execAsBatch() {
      return this.isBatch;
    }
  }

  /**
   * A thread that runs the database interactions asynchronously
   * from the OutputCollector.
   */
  public static class AsyncSqlExecThread extends Thread {

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
    public AsyncSqlExecThread(Connection conn, int stmtsPerTx) {
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
              if (op.execAsBatch()) {
                stmt.executeBatch();
              } else {
                stmt.execute();
              }
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
          } catch (BatchUpdateException batchE) {
            if (batchE.getNextException() != null) {
              // if a statement in a batch causes an SQLException
              // the database can either set it as the cause of
              // the BatchUpdateException, or set it as the 'next'
              // field of the BatchUpdateException (e.g. HSQLDB 1.8
              // does the former and Postgres 8.4 does the latter).
              // We'll check for this SQLException in both places,
              // and use the 'next' one in preference.
              setLastError(batchE.getNextException());
            } else {
              // same as SQLException block
              setLastError(batchE);
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
}
