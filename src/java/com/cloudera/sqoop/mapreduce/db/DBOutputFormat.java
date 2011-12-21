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
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.sqoop.mapreduce.DBWritable;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p>
 * {@link DBOutputFormat} accepts &lt;key,value&gt; pairs, where
 * key has a type extending DBWritable. Returned {@link RecordWriter}
 * writes <b>only the key</b> to the database with a batch SQL query.
 *
 * @deprecated use org.apache.sqoop.mapreduce.db.DBoutputFormat instead.
 * @see org.apache.sqoop.mapreduce.db.DBOutputFormat
 */
public class DBOutputFormat<K extends DBWritable, V>
    extends org.apache.sqoop.mapreduce.db.DBOutputFormat<K, V> {

  /**
   * A RecordWriter that writes the reduce output to a SQL table.
   *
   * @deprecated use
   *   org.apache.sqoop.mapreduce.db.DBOutputFormat.DBRecordWriter instead.
   * @see org.apache.sqoop.mapreduce.db.DBOutputFormat.DBRecordWriter
   */
  public static class DBRecordWriter<K extends DBWritable, V> extends
    org.apache.sqoop.mapreduce.db.DBOutputFormat.DBRecordWriter<K, V> {

    public DBRecordWriter() throws SQLException {
      super();
    }

    public DBRecordWriter(Connection connection,
        PreparedStatement statement) throws SQLException {
      super(connection, statement);
    }
  }
}
