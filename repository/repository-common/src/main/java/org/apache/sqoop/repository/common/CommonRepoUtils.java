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
package org.apache.sqoop.repository.common;

public class CommonRepoUtils {
  public static final String QUOTE_CHARACTER = "\"";

  public static final String escapeTableName(String tableName) {
    return QUOTE_CHARACTER + tableName + QUOTE_CHARACTER;
  }

  public static final String escapeColumnName(String columnName) {
    return QUOTE_CHARACTER + columnName + QUOTE_CHARACTER;
  }

  public static final String escapeSchemaName(String schemaName) {
    return QUOTE_CHARACTER + schemaName + QUOTE_CHARACTER;
  }

  public static final String escapeConstraintName(String constraintName) {
    return QUOTE_CHARACTER + constraintName + QUOTE_CHARACTER;
  }

  public static final String getTableName(String schemaName, String tableName) {
    if (schemaName != null) {
      return escapeSchemaName(schemaName) + "." + escapeTableName(tableName);
    } else {
      return escapeTableName(tableName);
    }
  }

  public static final String getColumnName(String tableName, String columnName) {
    if (tableName != null) {
      return escapeTableName(tableName) + "." + escapeColumnName(columnName);
    } else {
      return escapeColumnName(columnName);
    }
  }

  public static final String getConstraintName(String schemaName, String constraintName) {
    if (schemaName != null) {
      return escapeSchemaName(schemaName) + "." + escapeConstraintName(constraintName);
    } else {
      return escapeConstraintName(constraintName);
    }
  }
}
