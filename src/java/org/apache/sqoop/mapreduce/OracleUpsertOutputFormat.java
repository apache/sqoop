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
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.UpdateOutputFormat;

/**
 * Update an existing table with new value if the table already
 * contains the row, or insert the data into the table if the table
 * does not contain the row yet.
 */
public class OracleUpsertOutputFormat<K extends SqoopRecord, V>
    extends UpdateOutputFormat<K, V> {

  private static final Log LOG =
      LogFactory.getLog(OracleUpsertOutputFormat.class);

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new OracleUpsertRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * RecordWriter to write the output to UPDATE/INSERT statements.
   */
  public class OracleUpsertRecordWriter extends UpdateRecordWriter {

    public OracleUpsertRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);
    }

    /**
     * @return an UPDATE/INSERT statement that modifies/inserts a row
     * depending on whether the row already exist in the table or not.
     */
    protected String getUpdateStatement() {
      boolean first;

      // lookup table for update columns
      Set<String> updateKeyLookup = new LinkedHashSet<String>();
      for (String updateKey : updateCols) {
        updateKeyLookup.add(updateKey);
      }

      StringBuilder sb = new StringBuilder();
      sb.append("MERGE INTO ");
      sb.append(tableName);
      sb.append(" USING dual ON ( ");
      first = true;
      for (int i = 0; i < updateCols.length; i++) {
        if (first) {
          first = false;
        } else {
          sb.append(" AND ");
        }
        sb.append(updateCols[i]).append(" = ?");
      }
      sb.append(" )");

      sb.append("  WHEN MATCHED THEN UPDATE SET ");
      first = true;
      for (String col : columnNames) {
        if (!updateKeyLookup.contains(col)) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(col);
          sb.append(" = ?");
        }
      }

      sb.append("  WHEN NOT MATCHED THEN INSERT ( ");
      first = true;
      for (String col : columnNames) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(col);
      }
      sb.append(" ) VALUES ( ");
      first = true;
      for (String col : columnNames) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append("?");
      }
      sb.append(" )");

      return sb.toString();
    }
  }
}
