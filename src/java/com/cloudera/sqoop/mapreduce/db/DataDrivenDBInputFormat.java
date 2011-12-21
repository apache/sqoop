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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.mapreduce.DBWritable;

/**
 * A InputFormat that reads input data from an SQL table.
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to
 * demarcate splits, it tries to generate WHERE clauses which separate the
 * data into roughly equivalent shards.
 *
 * @deprecated use org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat instead
 * @see org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat
 */
public class DataDrivenDBInputFormat<T extends DBWritable>
    extends org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat<T> {

  /**
   * If users are providing their own query, the following string is expected
   * to appear in the WHERE clause, which will be substituted with a pair of
   * conditions on the input to allow input splits to parallelise the import.
   */
  public static final String SUBSTITUTE_TOKEN =
      org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.SUBSTITUTE_TOKEN;

  /**
   * A InputSplit that spans a set of rows.
   *
   * @deprecated use org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.
   *   DataDrivenDBInputSplit instead.
   * @see org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.
   *      DataDrivenDBInputSplit
   */
  public static class DataDrivenDBInputSplit extends
  org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit {

    /**
     * Default Constructor.
     */
    public DataDrivenDBInputSplit() {
      super();
    }

    /**
     * Convenience Constructor.
     * @param lower the string to be put in the WHERE clause to guard
     * on the 'lower' end.
     * @param upper the string to be put in the WHERE clause to guard
     * on the 'upper' end.
     */
    public DataDrivenDBInputSplit(final String lower, final String upper) {
      super(lower, upper);
    }
  }


  /** Set the user-defined bounding query to use with a user-defined query.
      This *must* include the substring "$CONDITIONS"
      (DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) inside the WHERE clause,
      so that DataDrivenDBInputFormat knows where to insert split clauses.
      e.g., "SELECT foo FROM mytable WHERE $CONDITIONS"
      This will be expanded to something like:
        SELECT foo FROM mytable WHERE (id &gt; 100) AND (id &lt; 250)
      inside each split.
    */
  public static void setBoundingQuery(Configuration conf, String query) {
    org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.setBoundingQuery(
        conf, query);
  }

  // Configuration methods override superclass to ensure that the proper
  // DataDrivenDBInputFormat gets used.

  /** Note that the "orderBy" column is called the "splitBy" in this version.
    * We reuse the same field, but it's not strictly ordering it
    * -- just partitioning the results.
    */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String tableName, String conditions,
      String splitBy, String... fieldNames) {
    org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.setInput(
        job, inputClass, tableName, conditions, splitBy, fieldNames);
  }

  /** setInput() takes a custom query and a separate "bounding query" to use
      instead of the custom "count query" used by DBInputFormat.
    */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputBoundingQuery) {
    org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.setInput(
        job, inputClass, inputQuery, inputBoundingQuery);
  }
}
