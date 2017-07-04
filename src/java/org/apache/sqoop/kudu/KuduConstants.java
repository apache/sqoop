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

package org.apache.sqoop.kudu;

/**
 * This class provides constants used to define properties relating to
 * Kudu imports.
 */
public final class KuduConstants {

  /** Kudu key column delimiter to use by default. */
  public static final String KUDU_KEY_COLS_DELIMITER = ",";


  /** Should columns be set to Nullable by default. */
  public static final boolean KUDU_SET_NULLABLE_COLUMN_ALWAYS = true;

  /** Number of replicas to use by default for Kudu table. */
  public static final int KUDU_DEFAULT_NUM_REPLICAS = 3;

  /** Number of buckets to use by default for Kudu table. */
  public static final int KUDU_DEFAULT_NO_OF_BUCKETS = 3;

  //Prevent instantiation.
  private KuduConstants() {
  }

}
