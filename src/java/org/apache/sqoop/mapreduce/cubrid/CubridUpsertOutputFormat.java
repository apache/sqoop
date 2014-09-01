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
package org.apache.sqoop.mapreduce.cubrid;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.UpdateOutputFormat;

/**
 * Output format for CUBRID Update/insert functionality. We will use CUBID
 * clause INSERT INTO ... ON DUPLICATE KEY UPDATE, for more info please see
 * official CUBRID documentation.
 */
public class CubridUpsertOutputFormat<K extends SqoopRecord, V> extends
    UpdateOutputFormat<K, V> {

  private final Log log = LogFactory.getLog(getClass());

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new CubridUpsertRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * RecordWriter to write the output to UPDATE/INSERT statements.
   */
  public class CubridUpsertRecordWriter extends UpdateRecordWriter {

    public CubridUpsertRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getUpdateStatement() {
      boolean first;
      StringBuilder sb = new StringBuilder();
      sb.append("INSERT INTO ");
      sb.append(tableName);
      sb.append("(");
      first = true;
      for (String column : columnNames) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(column);
      }

      sb.append(") VALUES(");
      first = true;
      for (int i = 0; i < columnNames.length; i++) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append("?");
      }

      sb.append(") ON DUPLICATE KEY UPDATE ");

      first = true;
      for (String column : columnNames) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(column).append("=").append(column);
      }

      String query = sb.toString();
      log.debug("Using upsert query: " + query);
      return query;
    }
  }
}
