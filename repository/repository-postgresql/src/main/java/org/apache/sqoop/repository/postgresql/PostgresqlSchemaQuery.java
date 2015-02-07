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
package org.apache.sqoop.repository.postgresql;

import org.apache.sqoop.repository.common.CommonRepoUtils;
import org.apache.sqoop.repository.common.CommonRepositorySchemaConstants;

/**
 * DML for PostgreSQL repository.
 */
public class PostgresqlSchemaQuery {

  public static final String STMT_SELECT_SYSTEM =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_VALUE)
          + " FROM " + CommonRepoUtils.getTableName(CommonRepositorySchemaConstants.SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_KEY) + " = ?";

  public static final String STMT_DELETE_SYSTEM =
      "DELETE FROM "  + CommonRepoUtils.getTableName(CommonRepositorySchemaConstants.SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_KEY) + " = ?";

  public static final String STMT_INSERT_SYSTEM =
      "INSERT INTO " + CommonRepoUtils.getTableName(CommonRepositorySchemaConstants.SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME) + "("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_KEY) + ", "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_VALUE) + ") "
          + "VALUES(?, ?)";

  public static final String STMT_INSERT_DIRECTION =
      "INSERT INTO " + CommonRepoUtils.getTableName(CommonRepositorySchemaConstants.SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_DIRECTION_NAME)
          + " (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQD_NAME)+ ") VALUES (?)";

  private PostgresqlSchemaQuery() {
    // Disable explicit object creation
  }
}
