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

package com.cloudera.sqoop.mapreduce;

import java.sql.Connection;
import java.sql.PreparedStatement;
import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public abstract class AsyncSqlOutputFormat<K extends SqoopRecord, V>
    extends org.apache.sqoop.mapreduce.AsyncSqlOutputFormat<K, V> {

  public static final String RECORDS_PER_STATEMENT_KEY =
      org.apache.sqoop.mapreduce.AsyncSqlOutputFormat.
      RECORDS_PER_STATEMENT_KEY;

  public static final String STATEMENTS_PER_TRANSACTION_KEY =
      org.apache.sqoop.mapreduce.AsyncSqlOutputFormat.
      STATEMENTS_PER_TRANSACTION_KEY;

  public static final int DEFAULT_RECORDS_PER_STATEMENT =
      org.apache.sqoop.mapreduce.AsyncSqlOutputFormat.
      DEFAULT_RECORDS_PER_STATEMENT;

  public static final int DEFAULT_STATEMENTS_PER_TRANSACTION =
      org.apache.sqoop.mapreduce.AsyncSqlOutputFormat.
      DEFAULT_STATEMENTS_PER_TRANSACTION;

  public static final int UNLIMITED_STATEMENTS_PER_TRANSACTION =
      org.apache.sqoop.mapreduce.AsyncSqlOutputFormat.
      UNLIMITED_STATEMENTS_PER_TRANSACTION;

  /**
   * @deprecated Moving to use org.apache.sqoop namespace.
   */
  public static class AsyncDBOperation
      extends org.apache.sqoop.mapreduce.AsyncSqlOutputFormat.
      AsyncDBOperation {

    public AsyncDBOperation(PreparedStatement s, boolean commitAndClose,
        boolean batch) {
        super(s, commitAndClose, batch);
    }

    public AsyncDBOperation(PreparedStatement s, boolean batch,
        boolean commit, boolean stopThread) {
      super(s, batch, commit, stopThread);
    }

  }

  /**
   * @deprecated Moving to use org.apache.sqoop namespace.
   */
  public static class AsyncSqlExecThread
      extends org.apache.sqoop.mapreduce.AsyncSqlOutputFormat.
      AsyncSqlExecThread{

    public AsyncSqlExecThread(Connection conn, int stmtsPerTx) {
      super(conn, stmtsPerTx);
    }

  }
}
