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

package org.apache.sqoop.mapreduce.sqlserver;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.manager.SQLServerManager;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.UpdateOutputFormat;

/**
 * Update an existing table with new value if the table already
 * contains the row, or insert the data into the table if the table
 * does not contain the row yet.
 */
public class SqlServerUpsertOutputFormat<K extends SqoopRecord, V>
    extends UpdateOutputFormat<K, V> {

  private static final Log LOG =
      LogFactory.getLog(SqlServerUpsertOutputFormat.class);

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new SqlServerUpsertRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * RecordWriter to write the output to UPDATE/INSERT statements.
   */
  public class SqlServerUpsertRecordWriter extends UpdateRecordWriter {

    public SqlServerUpsertRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);
    }

    @Override
    /**
     * @return an UPDATE/INSERT statement that modifies/inserts a row
     * depending on whether the row already exist in the table or not.
     */
    protected String getUpdateStatement() {
      boolean first;
      List<String> updateKeyLookup = Arrays.asList(updateCols);
      StringBuilder sb = new StringBuilder();

      if (getConf().getBoolean(SQLServerManager.IDENTITY_INSERT_PROP, false)) {
        LOG.info("Enabling identity inserts");
        sb.append("SET IDENTITY_INSERT ").append(tableName).append(" ON ");
      }

      sb.append("MERGE INTO ").append(tableName).append(" AS _target");
      sb.append(" USING ( VALUES ( ");
      first = true;
      for (String col : columnNames) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append("?");
      }
      sb.append(" ) )").append(" AS _source ( ");
      first = true;
      for (String col : columnNames) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(col);
      }
      sb.append(" )");

      sb.append(" ON ");
      first = true;
      for (String updateCol : updateCols) {
        if (updateKeyLookup.contains(updateCol)) {
          if (first) {
            first = false;
          } else {
            sb.append(" AND ");
          }
          sb.append("_source.").append(updateCol).append(" = _target.").append(updateCol);
        }
      }

      sb.append("  WHEN MATCHED THEN UPDATE SET ");
      first = true;
      for (String col : columnNames) {
        if (!updateKeyLookup.contains(col)) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append("_target.").append(col).append(" = _source.").append(col);
        }
      }

      sb.append("  WHEN NOT MATCHED THEN ").append("INSERT ( ");
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
        sb.append("_source.").append(col);
      }
      sb.append(" )");

      String tableHints = getConf().get(org.apache.sqoop.manager.SQLServerManager.TABLE_HINTS_PROP);
      if (tableHints != null) {
        LOG.info("Using table hints for query hints: " + tableHints);
        sb.append(" OPTION (").append(tableHints).append(")");
      }

      sb.append(";");

      return sb.toString();
    }
  }
}
