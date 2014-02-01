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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.lib.SqoopRecord;

/**
 * Insert the emitted keys as records into a database table.
 * Insert failures are handled by the registered Failure Handler class which
 * allows for recovering from certain failures like intermittent connection
 * or database throttling, .. etc
 *
 * The number of records per transaction is governed by the
 * sqoop.export.records.per.statement configuration value or else default
 * value is used
 *
 * Record objects are buffered before actually performing the INSERT
 * statements; this requires that the key implement the
 * SqoopRecord interface.
 */
public class SQLServerResilientExportOutputFormat<K extends SqoopRecord, V>
    extends OutputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(
      SQLServerResilientExportOutputFormat.class);

  public static final String EXPORT_FAILURE_HANDLER_CLASS =
      "sqoop.export.failure.handler.class";

  public static final int DEFAULT_RECORDS_PER_STATEMENT = 1000;

  private int curListIdx = 0;

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
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new SQLServerExportRecordWriter<K, V>(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  /** {@inheritDoc} */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new NullOutputCommitter();
  }

  /**
   * RecordWriter to write the output to a row in a database table.
   * The actual database updates are executed in a parallel thread in a
   * resilient fashion which attempts to recover failed operations
   */
  public class SQLServerExportRecordWriter<K extends SqoopRecord, V>
      extends RecordWriter<K, V> {

    private final Log LOG = LogFactory.getLog(
        SQLServerExportRecordWriter.class);
    private final int LIST_COUNT = 2;
    protected Configuration conf;
    protected SQLServerAsyncDBExecThread execThread;

    // Number of records to buffer before sending as a batch
    protected int recordsPerStmt;

    // We alternate between 2 lists of records as we go, as one is sent to the
    // target database the other gets asynchronously filled
    protected List<List<SqoopRecord>> recordsLists = new ArrayList<List<SqoopRecord>>();
    protected List<SqoopRecord> currentList;
    public SQLServerExportRecordWriter(TaskAttemptContext context)
        throws IOException {
      conf = context.getConfiguration();

      recordsPerStmt = conf.getInt(
        AsyncSqlOutputFormat.RECORDS_PER_STATEMENT_KEY,
        DEFAULT_RECORDS_PER_STATEMENT);

      // Create the lists to host incoming records
      List<SqoopRecord> newList;
      for (int i = 0; i < LIST_COUNT; ++i) {
        newList = new ArrayList<SqoopRecord>(recordsPerStmt);
        recordsLists.add(newList);
      }
      currentList = recordsLists.get(0);
      // Initialize the DB exec Thread
      initializeExecThread();

      // Start the DB exec thread
      execThread.start();
    }

    /**
     * Initialize the thread used to perform the asynchronous DB operation
     */
    protected void initializeExecThread() throws IOException {
      execThread = new SQLServerExportDBExecThread();
      execThread.initialize(conf);
    }

    @Override
    /** {@inheritDoc} */
    public void write(K key, V value)
        throws InterruptedException, IOException {
      try {
        currentList.add((SqoopRecord) key.clone());
        if (currentList.size() >= this.recordsPerStmt) {
          // Schedule the current list for asynchronous transfer
          // This will block if the previous operation is still in progress
          execThread.put(currentList);

          // Switch to the other list for receiving incoming records
          curListIdx = (curListIdx + 1) % recordsLists.size();

          // Clear the list to be used in case it has previous records
          currentList = recordsLists.get(curListIdx);
          currentList.clear();
        }
      } catch (CloneNotSupportedException cnse) {
        throw new IOException("Could not buffer record", cnse);
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
      try {
        // Ensure we flush the list of records to the database
        if (currentList.size() > 0) {
          execThread.put(currentList);
        }
      }
      finally {
        execThread.close();
        execThread.join();
      }

      // Final check for any exceptions raised when writing to the database
      Exception lastException = execThread.getLastError();
      if (lastException != null) {
        LOG.error("Asynchronous writer thread encountered the following " +
          "exception: " + lastException.toString());
        throw new IOException(lastException);
      }
    }
  }
}
