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

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_CONFIG;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_EDITABLE;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_ENUMVALS;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_ID;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_INDEX;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_NAME;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_STRLENGTH;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_STRMASK;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQI_TYPE;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.SCHEMA_SQOOP;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.TABLE_SQ_INPUT_NAME;

import org.apache.sqoop.repository.common.CommonRepoUtils;
import org.apache.sqoop.repository.common.CommonRepositoryInsertUpdateDeleteSelectQuery;

public class MysqlRepositoryInsertUpdateDeleteSelectQuery extends
    CommonRepositoryInsertUpdateDeleteSelectQuery {

  // DML: Get inputs for a given config
  // MySQL requires that we cast to null to char instead of varchar
  private static final String STMT_SELECT_INPUT = "SELECT "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRMASK) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRLENGTH) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_EDITABLE) + ", "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ENUMVALS) + ", "
      + "cast(null as char(100)),"
      + "false,"
      + "cast(null as char(100)),"
      + "cast(null as char(100))" + " FROM "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
      + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG)
      + " = ?" + " ORDER BY "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX);

  @Override
  public String getStmtSelectInput() {
    return STMT_SELECT_INPUT;
  }
}