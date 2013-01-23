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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.ExportOutputFormat;

/**
 * This class uses batch mode to execute underlying statements instead of
 * using a single multirow insert statement as its superclass.
 */
public class ExportBatchOutputFormat<K extends SqoopRecord, V>
    extends ExportOutputFormat<K, V> {

  private static final Log LOG =
      LogFactory.getLog(ExportBatchOutputFormat.class);

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new ExportBatchRecordWriter<K, V>(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * RecordWriter to write the output to a row in a database table.
   * The actual database updates are executed in a second thread.
   */
  public class ExportBatchRecordWriter<K extends SqoopRecord, V>
    extends ExportRecordWriter<K, V> {

    public ExportBatchRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);
    }

    @Override
    /** {@inheritDoc} */
    protected boolean isBatchExec() {
      // We use batches here.
      return true;
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
        stmt = conn.prepareStatement(getInsertStatement(userRecords.size()));
      }

      // Inject the record parameters into the VALUES clauses.
      for (SqoopRecord record : userRecords) {
        record.write(stmt, 0);
        stmt.addBatch();
      }

      return stmt;
    }

    /**
     * @return an INSERT statement.
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
  }
}
