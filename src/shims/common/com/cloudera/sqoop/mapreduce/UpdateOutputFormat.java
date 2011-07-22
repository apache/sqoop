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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;

import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * Update an existing table of data with new value data.
 * This requires a designated 'key column' for the WHERE clause
 * of an UPDATE statement.
 *
 * Updates are executed en batch in the PreparedStatement.
 *
 * Uses DBOutputFormat/DBConfiguration for configuring the output.
 */
public class UpdateOutputFormat<K extends SqoopRecord, V> 
    extends AsyncSqlOutputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(UpdateOutputFormat.class);

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
      return new UpdateRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * RecordWriter to write the output to UPDATE statements modifying rows
   * in the database.
   */
  public class UpdateRecordWriter extends AsyncSqlRecordWriter<K, V> {

    private String tableName;
    private String [] columnNames; // The columns to update.
    private String updateCol; // The column containing the fixed key.

    public UpdateRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);

      Configuration conf = getConf();

      DBConfiguration dbConf = new DBConfiguration(conf);
      this.tableName = dbConf.getOutputTableName();
      this.columnNames = dbConf.getOutputFieldNames();
      this.updateCol = conf.get(ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY);
    }

    @Override
    /** {@inheritDoc} */
    protected boolean isBatchExec() {
      // We use batches here.
      return true;
    }

    /**
     * @return the name of the table we are inserting into.
     */
    protected final String getTableName() {
      return tableName;
    }

    /**
     * @return the list of columns we are updating.
     */
    protected final String [] getColumnNames() {
      if (null == columnNames) {
        return null;
      } else {
        return Arrays.copyOf(columnNames, columnNames.length);
      }
    }
    
    /**
     * @return the column we are using to determine the row to update.
     */
    protected final String getUpdateCol() {
      return updateCol;
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
        stmt = conn.prepareStatement(getUpdateStatement());
      }

      // Inject the record parameters into the UPDATE and WHERE clauses.  This
      // assumes that the update key column is the last column serialized in
      // by the underlying record. Our code auto-gen process for exports was
      // responsible for taking care of this constraint.
      for (SqoopRecord record : userRecords) {
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
      sb.append(this.updateCol);
      sb.append("=?");
      return sb.toString();
    }
  }
}
