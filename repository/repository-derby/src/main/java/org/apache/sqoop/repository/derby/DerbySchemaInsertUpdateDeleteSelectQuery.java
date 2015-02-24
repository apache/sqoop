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
package org.apache.sqoop.repository.derby;

import org.apache.sqoop.repository.common.CommonRepoUtils;
import org.apache.sqoop.repository.common.CommonRepositoryInsertUpdateDeleteSelectQuery;

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.*;
import static org.apache.sqoop.repository.derby.DerbySchemaConstants.*;


/**
 * Derby Repository Insert/ Update/ Delete / Select queries
 *
 */
public final class DerbySchemaInsertUpdateDeleteSelectQuery extends CommonRepositoryInsertUpdateDeleteSelectQuery {

  /******** SYSTEM TABLE**************/
  // DML: Get system key
  public static final String STMT_SELECT_SYSTEM =
    "SELECT "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQM_VALUE)
    + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SYSTEM_NAME)
    + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQM_KEY) + " = ?";

  //DML: Get deprecated or the new repo version system key
  public static final String STMT_SELECT_DEPRECATED_OR_NEW_SYSTEM_VERSION =
    "SELECT "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQM_VALUE) + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SYSTEM_NAME)
    + " WHERE ( " + CommonRepoUtils.escapeColumnName(COLUMN_SQM_KEY) + " = ? )"
    + " OR  (" + CommonRepoUtils.escapeColumnName(COLUMN_SQM_KEY) + " = ? )";

  // DML: Remove system key
  public static final String STMT_DELETE_SYSTEM =
    "DELETE FROM "  + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SYSTEM_NAME)
    + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQM_KEY) + " = ?";

  // DML: Insert new system key
  public static final String STMT_INSERT_SYSTEM =
    "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SYSTEM_NAME) + "("
    + CommonRepoUtils.escapeColumnName(COLUMN_SQM_KEY) + ", "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQM_VALUE) + ") "
    + "VALUES(?, ?)";

  /*********CONFIGURABLE TABLE ***************/
  // DML: Select all connectors
  @Deprecated // used only for upgrade logic
  public static final String STMT_SELECT_CONNECTOR_ALL =
     "SELECT "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQC_CLASS) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION)
     + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME);

   @Deprecated // used only in the upgrade path
   public static final String STMT_INSERT_INTO_CONNECTOR_WITHOUT_SUPPORTED_DIRECTIONS =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME)+ " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_CLASS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION)
          + ") VALUES (?, ?, ?)";

  //DML: Insert new connection
  @Deprecated // used only in upgrade path
  public static final String STMT_INSERT_CONNECTION =
    "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " ("
     + CommonRepoUtils.escapeColumnName(COLUMN_SQN_NAME) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CONNECTOR) + ","
     + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ENABLED) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CREATION_USER) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CREATION_DATE) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQN_UPDATE_USER) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQN_UPDATE_DATE)
     + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  /******* CONFIG and CONNECTOR DIRECTIONS ****/
  public static final String STMT_INSERT_DIRECTION = "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_DIRECTION_NAME) + " "
       + "(" + CommonRepoUtils.escapeColumnName(COLUMN_SQD_NAME) + ") VALUES (?)";

  public static final String STMT_FETCH_CONFIG_DIRECTIONS =
       "SELECT "
           + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID) + ", "
           + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIRECTION)
           + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME);

  public static final String STMT_FETCH_TABLE_FOREIGN_KEYS = "SELECT "
      + CommonRepoUtils.escapeColumnName("CONSTRAINTNAME")
      + " FROM " + CommonRepoUtils.getTableName("SYS", "SYSCONSTRAINTS")
      + " C LEFT JOIN " + CommonRepoUtils.getTableName("SYS", "SYSTABLES")
      + " T ON C." + CommonRepoUtils.escapeColumnName("TABLEID") + " = T." + CommonRepoUtils.escapeColumnName("TABLEID")
      +  " WHERE C.TYPE = 'F' AND T." + CommonRepoUtils.escapeColumnName("TABLENAME") + " = ?";

  private DerbySchemaInsertUpdateDeleteSelectQuery() {
    // Disable explicit object creation
  }
}