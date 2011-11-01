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

package com.cloudera.sqoop.manager;

import com.cloudera.sqoop.SqoopOptions;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class OracleManager
    extends org.apache.sqoop.manager.OracleManager {

  public static final int ERROR_TABLE_OR_VIEW_DOES_NOT_EXIST =
    org.apache.sqoop.manager.OracleManager.ERROR_TABLE_OR_VIEW_DOES_NOT_EXIST;
  public static final String QUERY_LIST_DATABASES =
    org.apache.sqoop.manager.OracleManager.QUERY_LIST_DATABASES;
  public static final String QUERY_LIST_TABLES =
    org.apache.sqoop.manager.OracleManager.QUERY_LIST_TABLES;
  public static final String QUERY_COLUMNS_FOR_TABLE =
    org.apache.sqoop.manager.OracleManager.QUERY_COLUMNS_FOR_TABLE;
  public static final String QUERY_PRIMARY_KEY_FOR_TABLE =
    org.apache.sqoop.manager.OracleManager.QUERY_PRIMARY_KEY_FOR_TABLE;
  public static final String ORACLE_TIMEZONE_KEY =
    org.apache.sqoop.manager.OracleManager.ORACLE_TIMEZONE_KEY;

  public OracleManager(final SqoopOptions opts) {
    super(opts);
  }

}

