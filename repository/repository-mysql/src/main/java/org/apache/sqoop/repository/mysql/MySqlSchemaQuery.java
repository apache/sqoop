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
package org.apache.sqoop.repository.mysql;

import org.apache.sqoop.repository.common.CommonRepoUtils;
import org.apache.sqoop.repository.common.CommonRepositorySchemaConstants;

/**
 * DML for MySql repository.
 */
public final class MySqlSchemaQuery {

  public static final String STMT_SELECT_SYSTEM =
      "SELECT "
          + CommonRepositorySchemaConstants.COLUMN_SQM_VALUE
          + " FROM " + CommonRepoUtils.getTableName(CommonRepositorySchemaConstants.SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME)
          + " WHERE " + CommonRepositorySchemaConstants.COLUMN_SQM_KEY + " = ?";

  public static final String STMT_INSERT_ON_DUPLICATE_KEY_SYSTEM =
      "INSERT INTO " + CommonRepoUtils.getTableName(CommonRepositorySchemaConstants.SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME) + "("
          + CommonRepositorySchemaConstants.COLUMN_SQM_KEY + ", "
          + CommonRepositorySchemaConstants.COLUMN_SQM_VALUE + ") "
          + "VALUES(?, ?) ON DUPLICATE KEY UPDATE " + CommonRepositorySchemaConstants.COLUMN_SQM_VALUE + " = ?";

  public static final String STMT_INSERT_DIRECTION =
      "INSERT INTO " + CommonRepoUtils.getTableName(CommonRepositorySchemaConstants.SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_DIRECTION_NAME)
          + " (" + CommonRepositorySchemaConstants.COLUMN_SQD_NAME+ ") VALUES (?)";

  private MySqlSchemaQuery() {
    // disable explicit object creation
  }
}
