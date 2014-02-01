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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.db.DBConfiguration;

/**
 * Update an existing table of data with new value data.
 * This requires a designated 'key column' for the WHERE clause
 * of an UPDATE statement.
 *
 * The number of records per transaction is governed by the
 * sqoop.export.records.per.statement configuration value or else default
 * value is used
 *
 * Record objects are buffered before actually performing the UPDATE
 * statements; this requires that the key implement the
 * SqoopRecord interface.
 */
public class SQLServerResilientUpdateOutputFormat<K extends SqoopRecord, V>
    extends SQLServerResilientExportOutputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(
    SQLServerResilientUpdateOutputFormat.class);

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
      throw new IOException("Table name is not set for export.");
    } else if (null == dbConf.getOutputFieldNames()) {
      throw new IOException(
          "Output field names are null.");
    } else if (null == conf.get(ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY)) {
      throw new IOException("Update key column is not set for export.");
    }
  }

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new SQLServerUpdateRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * RecordWriter to write the output to UPDATE statements modifying rows
   * in the database.
   * The actual database updates are executed in a parallel thread in a
   * resilient fashion which attempts to recover failed operations
   */
  public class SQLServerUpdateRecordWriter<K extends SqoopRecord, V>
      extends SQLServerExportRecordWriter<K, V> {

    private final Log LOG = LogFactory.getLog(
      SQLServerUpdateRecordWriter.class);

    public SQLServerUpdateRecordWriter(TaskAttemptContext context)
        throws IOException {
      super(context);
    }

    /**
     * Initialize the thread used to perform the asynchronous DB operation
     */
    protected void initializeExecThread() throws IOException {
      execThread = new SQLServerUpdateDBExecThread();
      execThread.initialize(conf);
    }
  }
}
