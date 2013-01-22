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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.mapreduce.db.DBConfiguration;

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
public class ExportCallOutputFormat<K extends SqoopRecord, V>
    extends AsyncSqlOutputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(
      ExportCallOutputFormat.class);

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
      throw new IOException("Procedure name is not set for export");
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
      return new ExportCallRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * RecordWriter to write the output to a row in a database table.
   * The actual database updates are executed in a second thread.
   */
  public class ExportCallRecordWriter extends AsyncSqlRecordWriter<K, V> {

    protected String procedureName;
    protected String [] columnNames; // The columns to insert into.
    protected int columnCount; // If columnNames is null, tells ## of cols.

    public ExportCallRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);

      Configuration conf = getConf();

      DBConfiguration dbConf = new DBConfiguration(conf);
      procedureName = dbConf.getOutputTableName();
      columnNames = dbConf.getOutputFieldNames();
      columnCount = dbConf.getOutputFieldCount();
    }

    @Override
    /** {@inheritDoc} */
    protected PreparedStatement getPreparedStatement(
        List<SqoopRecord> userRecords) throws SQLException {

      PreparedStatement stmt = null;

      // Synchronize on connection to ensure this does not conflict
      // with the operations in the update thread.
      Connection conn = getConnection();
      synchronized (conn) {
        stmt = conn.prepareCall(getCallStatement(userRecords.size()));
      }

      for (SqoopRecord record : userRecords) {
        record.write(stmt, 0);
        stmt.addBatch();
      }

      return stmt;
    }

    @Override
    protected boolean isBatchExec() {
      return true;
    }

    /**
     * @return an INSERT statement suitable for inserting 'numRows' rows.
     */
    protected String getCallStatement(int numRows) {
      StringBuilder sb = new StringBuilder();

      sb.append("{call " + procedureName + " (");

      int numSlots = columnNames == null ? columnCount : columnNames.length;
      if (numSlots > 0) {
        sb.append("?");
      }
      for(int i = 1; i < numSlots; ++i) {
        sb.append(", ?");
      }

      sb.append(")}");

      return sb.toString();
    }
  }
}
