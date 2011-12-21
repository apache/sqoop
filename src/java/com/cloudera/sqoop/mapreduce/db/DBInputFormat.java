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

import org.apache.sqoop.mapreduce.DBWritable;

/**
 * A InputFormat that reads input data from an SQL table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as
 * key and DBWritables as value.
 *
 * The SQL query, and input class can be using one of the two
 * setInput methods.
 *
 * @deprecated use org.apache.sqoop.mapreduce.db.DBInputFormat instead.
 * @see org.apache.sqoop.mapreduce.db.DBInputFormat
 */
public class DBInputFormat<T extends DBWritable>
    extends org.apache.sqoop.mapreduce.db.DBInputFormat<T> {

  /**
   * A Class that does nothing, implementing DBWritable.
   * @deprecated use org.apache.sqoop.mapreduce.db.DBInputFormat.NullDBWritable
   *   instead.
   * @see org.apache.sqoop.mapreduce.db.DBInputFormat.NullDBWritable
   */
  public static class NullDBWritable
    extends org.apache.sqoop.mapreduce.db.DBInputFormat.NullDBWritable {
  }

  /**
   * A InputSplit that spans a set of rows.
   *
   * @deprecated use org.apache.sqoop.mapreduce.db.DBInputFormat.DBInputSplit
   *   instead.
   * @see org.apache.sqoop.mapreduce.db.DBInputFormat.DBInputSplit
   */
  public static class DBInputSplit extends
    org.apache.sqoop.mapreduce.db.DBInputFormat.DBInputSplit {

    /**
     * Default Constructor.
     */
    public DBInputSplit() {
      super();
    }

    /**
     * Convenience Constructor.
     * @param start the index of the first row to select
     * @param end the index of the last row to select
     */
    public DBInputSplit(long start, long end) {
      super(start, end);
    }
  }
}
