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
package org.apache.sqoop.connector.jdbc;

/**
 *
 */
public final class GenericJdbcConnectorConstants {

  // Resource bundle name
  public static final String RESOURCE_BUNDLE_NAME =
      "generic-jdbc-connector-config";

  /*
   * All jdbc connector related configuration is prefixed with this:
   * <tt>org.apache.sqoop.jdbc.</tt>
   */
  public static final String PREFIX_CONNECTOR_JDBC_CONFIG =
      "org.apache.sqoop.connector.jdbc.";

  public static final String CONNECTOR_JDBC_PARTITION_COLUMNNAME =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.columnname";
  public static final String CONNECTOR_JDBC_PARTITION_COLUMNTYPE =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.columntype";
  public static final String CONNECTOR_JDBC_PARTITION_MINVALUE =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.minvalue";
  public static final String CONNECTOR_JDBC_PARTITION_MAXVALUE =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.maxvalue";
  public static final String CONNECTOR_JDBC_LAST_INCREMENTAL_VALUE =
    PREFIX_CONNECTOR_JDBC_CONFIG + "incremental.last_value";

  public static final String CONNECTOR_JDBC_FROM_DATA_SQL =
      PREFIX_CONNECTOR_JDBC_CONFIG + "from.data.sql";
  public static final String CONNECTOR_JDBC_TO_DATA_SQL =
      PREFIX_CONNECTOR_JDBC_CONFIG + "to.data.sql";

  public static final String SQL_CONDITIONS_TOKEN = "${CONDITIONS}";

  public static final String SQL_PARAMETER_MARKER = "?";

  public static final String SUBQUERY_ALIAS = "SQOOP_SUBQUERY_ALIAS";

  private GenericJdbcConnectorConstants() {
    // Disable explicit object creation
  }
}
