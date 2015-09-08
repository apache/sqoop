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

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQB_ID;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQC_NAME;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQ_LNK_ID;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQ_LNK_NAME;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQB_NAME;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.SCHEMA_SQOOP;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.TABLE_SQ_CONFIGURABLE_NAME;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.TABLE_SQ_JOB_NAME;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.TABLE_SQ_LINK_NAME;

import org.apache.sqoop.repository.common.CommonRepoUtils;

public class PostgresqlSchemaUpgradeQuery {
  public static final String QUERY_UPGRADE_TABLE_SQ_LINK_UPDATE_COLUMN_SQ_LINK_NAME =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
        + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME)
        + " = " + CommonRepoUtils.escapeLiteralString("link_") + " || "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID)
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + " IS NULL";

  public static final String QUERY_UPGRADE_TABLE_SQ_LINK_ALTER_COLUMN_SQ_LINK_NAME_NOT_NULL =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
        + " ALTER COLUMN " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME)
        + " SET NOT NULL";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_COLUMN_SQB_NAME =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
        + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME)
        + " = " + CommonRepoUtils.escapeLiteralString("job_") + " || "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID)
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + " IS NULL";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ALTER_COLUMN_SQB_NAME_NOT_NULL =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
        + " ALTER COLUMN " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME)
        + " SET NOT NULL";

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ALTER_COLUMN_SQB_NAME_NOT_NULL =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
      + " ALTER COLUMN " + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME)
      + " SET NOT NULL";
}
