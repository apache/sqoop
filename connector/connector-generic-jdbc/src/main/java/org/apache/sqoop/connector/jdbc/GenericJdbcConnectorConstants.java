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
      "generic-jdbc-connector-resources";

  // Metadata constants

  // Connection form
  public static final String FORM_CONNECTION = "form-connection";

  // Connection form inputs
  public static final String INPUT_CONN_JDBCDRIVER = "inp-conn-jdbcdriver";
  public static final String INPUT_CONN_CONNECTSTRING =
      "inp-conn-connectstring";
  public static final String INPUT_CONN_USERNAME = "inp-conn-username";
  public static final String INPUT_CONN_PASSWORD = "inp-conn-password";
  public static final String INPUT_CONN_JDBCPROPS = "inp-conn-jdbc-properties";

  // Table form
  public static final String FORM_TABLE = "form-table";

  // Table form inputs
  public static final String INPUT_TBL_NAME = "inp-tbl-name";
  public static final String INPUT_TBL_SQL = "inp-tbl-sql";
  public static final String INPUT_TBL_COLUMNS = "inp-tbl-columns";
  public static final String INPUT_TBL_WAREHOUSE = "inp-tbl-warehouse";
  public static final String INPUT_TBL_DATADIR = "inp-tbl-datadir";
  public static final String INPUT_TBL_PCOL = "inp-tbl-pcol";
  public static final String INPUT_TBL_BOUNDARY = "inp-tbl-boundary";


  /*
   * All jdbc connector related configuration is prefixed with this:
   * <tt>org.apache.sqoop.jdbc.</tt>
   */
  public static final String PREFIX_CONNECTOR_JDBC_CONFIG =
      "org.apache.sqoop.connector.jdbc.";

  public static final String CONNECTOR_JDBC_DRIVER =
      PREFIX_CONNECTOR_JDBC_CONFIG + "driver";
  public static final String CONNECTOR_JDBC_URL =
      PREFIX_CONNECTOR_JDBC_CONFIG + "url";
  public static final String CONNECTOR_JDBC_USERNAME =
      PREFIX_CONNECTOR_JDBC_CONFIG + "username";
  public static final String CONNECTOR_JDBC_PASSWORD =
      PREFIX_CONNECTOR_JDBC_CONFIG + "password";

  public static final String CONNECTOR_JDBC_PARTITION_COLUMNNAME =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.columnname";
  public static final String CONNECTOR_JDBC_PARTITION_COLUMNTYPE =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.columntype";
  public static final String CONNECTOR_JDBC_PARTITION_MINVALUE =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.minvalue";
  public static final String CONNECTOR_JDBC_PARTITION_MAXVALUE =
      PREFIX_CONNECTOR_JDBC_CONFIG + "partition.maxvalue";

  public static final String CONNECTOR_JDBC_DATA_SQL =
      PREFIX_CONNECTOR_JDBC_CONFIG + "data.sql";

  public static final String FILE_SEPARATOR = System.getProperty("file.separator");

  public static final String DEFAULT_WAREHOUSE = "/tmp/sqoop/warehouse/";

  public static final String DEFAULT_DATADIR = "DataStore";

  public static final String SQL_CONDITIONS_TOKEN = "${CONDITIONS}";

  public static final String SQL_PARAMETER_MARKER = "?";

  public static final String SUBQUERY_ALIAS = "SQOOP_SUBQUERY_ALIAS";

  private GenericJdbcConnectorConstants() {
    // Disable explicit object creation
  }
}
