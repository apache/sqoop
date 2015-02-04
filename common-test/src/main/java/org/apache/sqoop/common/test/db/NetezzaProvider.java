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
 * Netezza Provider that will connect to remote Netezza server.
 *
 * JDBC can be configured via system properties. Default value is server running
 * on the same box (localhost) that is access via sqoop/sqoop credentials.
 */
public class NetezzaProvider extends DatabaseProvider {

  public static final String DRIVER = "org.netezza.Driver";

  private static final String CONNECTION = System.getProperties().getProperty(
    "sqoop.provider.netezza.jdbc",
    "jdbc:netezza://localhost/test"
  );

  private static final String USERNAME = System.getProperties().getProperty(
    "sqoop.provider.netezza.username",
    "sqoop"
  );

  private static final String PASSWORD = System.getProperties().getProperty(
    "sqoop.provider.netezza.password",
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
    return escapeObjectName(columnName);
  }

  @Override
  public String escapeTableName(String tableName) {
    return escapeObjectName(tableName);
  }

  @Override
  public String escapeSchemaName(String schemaName) {
    return schemaName;
  }

  public String escapeObjectName(String name) {
    return '"' + name + '"';
  }

  @Override
  public String escapeValueString(String value) {
    return "'" + value + "'";
  }

  @Override
  public String getJdbcDriver() {
    return DRIVER;
  }
}
