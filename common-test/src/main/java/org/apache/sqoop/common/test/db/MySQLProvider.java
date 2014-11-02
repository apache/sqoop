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
package org.apache.sqoop.common.test.db;

/**
 * MySQL Provider that will connect to remote MySQL server.
 *
 * JDBC can be configured via system properties. Default value is server running
 * on the same box (localhost) that is access via sqoop/sqoop credentials.
 */
public class MySQLProvider extends DatabaseProvider {

  public static final String DRIVER = "com.mysql.jdbc.Driver";

  private static final String CONNECTION = System.getProperties().getProperty(
    "sqoop.provider.mysql.jdbc",
    "jdbc:mysql://localhost/test"
  );

  private static final String USERNAME = System.getProperties().getProperty(
    "sqoop.provider.mysql.username",
    "sqoop"
  );

  private static final String PASSWORD = System.getProperties().getProperty(
    "sqoop.provider.mysql.password",
    "sqoop"
  );

  @Override
  public String getConnectionUrl() {
    return CONNECTION;
  }

  @Override
  public String getConnectionUsername() {
    return USERNAME;
  }

  @Override
  public String getConnectionPassword() {
    return PASSWORD;
  }

  @Override
  public String escapeColumnName(String columnName) {
    return escape(columnName);
  }

  @Override
  public String escapeTableName(String tableName) {
    return escape(tableName);
  }

  @Override
  public String escapeValueString(String value) {
    return "\"" + value + "\"";
  }

  @Override
  public String getJdbcDriver() {
    return DRIVER;
  }

  public String escape(String entity) {
    return "`" + entity + "`";
  }
}
