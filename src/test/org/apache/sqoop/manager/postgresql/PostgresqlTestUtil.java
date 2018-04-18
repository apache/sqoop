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

package org.apache.sqoop.manager.postgresql;

public class PostgresqlTestUtil {

  public static final String HOST_URL = System.getProperty(
      "sqoop.test.postgresql.connectstring.host_url",
      "jdbc:postgresql://localhost/");

  public static final String DATABASE_USER = System.getProperty(
      "sqoop.test.postgresql.username",
      "sqooptest");

  public static final String DATABASE_NAME = System.getProperty(
      "sqoop.test.postgresql.database",
      "sqooptest");

  public static final String CONNECT_STRING = HOST_URL + DATABASE_NAME;

  public static final String PASSWORD = System.getProperty(
      "sqoop.test.postgresql.password");

  static final String TABLE_NAME = "EMPLOYEES_PG";

  static final String NULL_TABLE_NAME = "NULL_EMPLOYEES_PG";

  static final String SPECIAL_TABLE_NAME = "EMPLOYEES_PG's";

  static final String DIFFERENT_TABLE_NAME = "DIFFERENT_TABLE";

  public static final String SCHEMA_PUBLIC = "public";

  public static final String SCHEMA_SPECIAL = "special";

  public static String quoteTableOrSchemaName(String tableName) {
    return "\"" + tableName + "\"";
  }

  public static String getDropTableStatement(String tableName, String schema) {
    return "DROP TABLE IF EXISTS " + quoteTableOrSchemaName(schema) + "." + quoteTableOrSchemaName(tableName);
  }
}
