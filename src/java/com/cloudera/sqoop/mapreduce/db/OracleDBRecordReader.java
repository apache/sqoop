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

/**
 * A RecordReader that reads records from an Oracle SQL table.
 * @deprecated use org.apache.sqoop.mapreduce.db.OracleDBRecordReader instead.
 * @see org.apache.sqoop.mapreduce.db.OracleDBRecordReader
 */
public class OracleDBRecordReader<T extends DBWritable> extends
  org.apache.sqoop.mapreduce.db.OracleDBRecordReader<T> {

  /** Configuration key to set to a timezone string. */
  public static final String SESSION_TIMEZONE_KEY =
      org.apache.sqoop.mapreduce.db.OracleDBRecordReader.SESSION_TIMEZONE_KEY;

  // CHECKSTYLE:OFF
  public OracleDBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn,
      DBConfiguration dbConfig, String cond, String [] fields,
      String table) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
  }
  // CHECKSTYLE:ON

  /**
   * Set session time zone.
   * @param conf The current configuration.
   * We read the 'oracle.sessionTimeZone' property from here.
   * @param conn The connection to alter the timezone properties of.
   */
  public static void setSessionTimeZone(Configuration conf,
      Connection conn) throws SQLException {
    org.apache.sqoop.mapreduce.db.OracleDBRecordReader.setSessionTimeZone(
        conf, conn);
  }
}
