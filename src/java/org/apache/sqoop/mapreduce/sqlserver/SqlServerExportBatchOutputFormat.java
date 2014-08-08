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

import com.cloudera.sqoop.lib.SqoopRecord;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.manager.SQLServerManager;
import org.apache.sqoop.mapreduce.ExportBatchOutputFormat;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Output format specific for Microsoft SQL Connector.
 */
public class SqlServerExportBatchOutputFormat<K extends SqoopRecord, V>
  extends ExportBatchOutputFormat<K, V> {

  private static final Log LOG =
    LogFactory.getLog(SqlServerExportBatchOutputFormat.class);

  /** {@inheritDoc} */
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new SqlServerExportBatchRecordWriter<K, V>(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /** {@inheritDoc}. */
  public class SqlServerExportBatchRecordWriter<K extends SqoopRecord, V>
    extends ExportBatchRecordWriter<K, V>{

    public SqlServerExportBatchRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    protected String getInsertStatement(int numRows) {
      StringBuilder sb = new StringBuilder();

      if (getConf().getBoolean(SQLServerManager.IDENTITY_INSERT_PROP, false)) {
        LOG.info("Enabling identity inserts");
        sb.append("SET IDENTITY_INSERT ").append(tableName).append(" ON ");
      }

      sb.append("INSERT INTO " + tableName + " ");

      String tableHints = getConf().get(SQLServerManager.TABLE_HINTS_PROP);
      if (tableHints != null) {
        LOG.info("Using table hints: " + tableHints);
        sb.append(" WITH (").append(tableHints).append(") ");
      }

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

      String query = sb.toString();
      LOG.info("Using query " + query);

      return query;
    }
  }
}
