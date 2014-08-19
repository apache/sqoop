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

package com.cloudera.sqoop;

import org.apache.hadoop.conf.Configuration;

/**
 * @deprecated
 */
public class SqoopOptions
  extends org.apache.sqoop.SqoopOptions implements Cloneable {

  public static final String METASTORE_PASSWORD_KEY =
    org.apache.sqoop.SqoopOptions.METASTORE_PASSWORD_KEY;

  public static final boolean METASTORE_PASSWORD_DEFAULT =
    org.apache.sqoop.SqoopOptions.METASTORE_PASSWORD_DEFAULT;

  public static final int DEFAULT_NUM_MAPPERS =
    org.apache.sqoop.SqoopOptions.DEFAULT_NUM_MAPPERS;

  /** Selects in-HDFS destination file format. */
  public enum FileLayout {
    TextFile,
    SequenceFile,
    AvroDataFile,
    ParquetFile
  }

  /**
   * Incremental imports support two modes:
   * <ul>
   * <li>new rows being appended to the end of a table with an
   * incrementing id</li>
   * <li>new data results in a date-last-modified column being
   * updated to NOW(); Sqoop will pull all dirty rows in the next
   * incremental import.</li>
   * </ul>
   */
  public enum IncrementalMode {
    None,
    AppendRows,
    DateLastModified,
  }

  /**
   * Update mode option specifies how updates are performed when
   * new rows are found with non-matching keys in database.
   * It supports two modes:
   * <ul>
   * <li>UpdateOnly: This is the default. New rows are silently ignored.</li>
   * <li>AllowInsert: New rows are inserted into the database.</li>
   * </ul>
   */
  public enum UpdateMode {
    UpdateOnly,
    AllowInsert
  }

  public SqoopOptions() {
    super();
  }

  public SqoopOptions(Configuration conf) {
    super(conf);
  }

  public SqoopOptions(final String connect, final String table) {
    super(connect, table);
  }

  public static void clearNonceDir() {
    org.apache.sqoop.SqoopOptions.clearNonceDir();
  }

  public static String getHiveHomeDefault() {
    return org.apache.sqoop.SqoopOptions.getHiveHomeDefault();
  }

  /**
   * {@inheritDoc}.
   * @deprecated
   */
  public static class InvalidOptionsException
    extends org.apache.sqoop.SqoopOptions.InvalidOptionsException {


    public InvalidOptionsException(final String msg) {
      super(msg);
    }
  }

  public static char toChar(String charish) throws InvalidOptionsException {
    try {
      return org.apache.sqoop.SqoopOptions.toChar(charish);
    } catch(org.apache.sqoop.SqoopOptions.InvalidOptionsException ex) {
      throw new InvalidOptionsException(ex.getMessage());
    }
  }
}

