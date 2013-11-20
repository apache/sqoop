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
package org.apache.sqoop.accumulo;

/**
 * This class provides constants used to define properties relating to
 * Accumulo imports.
 */
public final class AccumuloConstants {
  // Some defaults to use if these aren't specified.
  // Default buffer size for BatchWriter
  public static final long DEFAULT_BATCH_SIZE = 10240000L;
  // Default latency for BatchWriter
  public static final long DEFAULT_LATENCY = 5000L;

  /** Configuration key specifying the table to insert into. */
  public static final String TABLE_NAME_KEY = "sqoop.accumulo.insert.table";

  /** Configuration key specifying the column family to insert into. */
  public static final String COL_FAMILY_KEY =
      "sqoop.accumulo.insert.column.family";

  /** Configuration key specifying the column of the input whose value
   * should be used as the row id.
   */
  public static final String ROW_KEY_COLUMN_KEY =
      "sqoop.accumulo.insert.row.key.column";

  /** Configuration key specifying the column of the input whose value
   * should be used as the row id.
   */
  public static final String VISIBILITY_KEY =
      "sqoop.accumulo.insert.visibility";
  /**
   * Configuration key specifying the Transformer implementation to use.
   */
  public static final String TRANSFORMER_CLASS_KEY =
      "sqoop.accumulo.insert.put.transformer.class";

  public static final String MAX_LATENCY =
          "sqoop.accumulo.max.latency";

  public static final String BATCH_SIZE =
          "sqoop.accumulo.batch.size";

  public static final String ZOOKEEPERS =
          "sqoop.accumulo.zookeeper.hostnames";

  public static final String ACCUMULO_INSTANCE =
          "sqoop.accumulo.instance.name";

  public static final String ACCUMULO_USER_NAME =
          "sqoop.accumulo.user.name";

  public static final String ACCUMULO_PASSWORD =
          "sqoop.accumulo.password";

  public static final String ACCUMULO_SITE_XML_PATH =
          "/conf/accumulo-site.xml";

  // Prevent instantiation.
  private AccumuloConstants(){
  }
}
