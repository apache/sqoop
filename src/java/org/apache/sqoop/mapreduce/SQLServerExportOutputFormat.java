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
import java.sql.SQLException;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.ExportOutputFormat;

/**
 * SQLServer-specific SQL formatting overrides default ExportOutputFormat's.
 */
public class SQLServerExportOutputFormat<K extends SqoopRecord, V>
    extends ExportOutputFormat<K, V> {

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

  /**
   * RecordWriter to write the output to a row in a database table.
   * The actual database updates are executed in a second thread.
   */
  public class SQLServerExportRecordWriter<K extends SqoopRecord, V>
    extends ExportRecordWriter<K, V> {

    public SQLServerExportRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);
    }

    @Override
    /**
     * @return an INSERT statement suitable for inserting 'numRows' rows.
     */
    protected String getInsertStatement(int numRows) {
      StringBuilder sb = new StringBuilder();

      sb.append("INSERT INTO " + getTableName() + " ");

      int numSlots;
      String [] colNames = getColumnNames();
      if (colNames != null) {
        numSlots = colNames.length;

        sb.append("(");
        boolean first = true;
        for (String col : colNames) {
          if (!first) {
            sb.append(", ");
          }

          sb.append(col);
          first = false;
        }

        sb.append(") ");
      } else {
        numSlots = getColumnCount(); // set if columnNames is null.
      }

      // generates the (?, ?, ?...) used for each row.
      StringBuilder sbRow = new StringBuilder();
      sbRow.append("(SELECT ");
      for (int i = 0; i < numSlots; i++) {
        if (i != 0) {
          sbRow.append(", ");
        }

        sbRow.append("?");
      }
      sbRow.append(") ");

      // Now append that numRows times.
      for (int i = 0; i < numRows; i++) {
        if (i != 0) {
          sb.append("UNION ALL ");
        }

        sb.append(sbRow);
      }

      return sb.toString();
    }
  }
}
