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
package com.cloudera.sqoop.mapreduce.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.mapreduce.db.DBConfiguration;

/**
 * A RecordReader that reads records from a SQL table.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 *
 * @deprecated use org.apache.sqoop.mapreduce.db.DBRecordReader instead.
 * @see org.apache.sqoop.mapreduce.db.DBRecordReader
 */
public class DBRecordReader<T extends DBWritable> extends
    org.apache.sqoop.mapreduce.db.DBRecordReader<T> {

  /**
   * @param split The InputSplit to read data for
   * @throws SQLException
   */
  // CHECKSTYLE:OFF
  // TODO (aaron): Refactor constructor to take fewer arguments
  public DBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn,
      DBConfiguration dbConfig, String cond, String [] fields, String table)
      throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
  }
  // CHECKSTYLE:ON
}
