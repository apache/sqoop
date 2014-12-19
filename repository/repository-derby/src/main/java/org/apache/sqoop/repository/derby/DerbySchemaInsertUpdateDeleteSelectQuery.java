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

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.*;
import static org.apache.sqoop.repository.derby.DerbySchemaConstants.*;


/**
 * Derby Repository Insert/ Update/ Delete / Select queries
 *
 */
public final class DerbySchemaInsertUpdateDeleteSelectQuery {

  /******** SYSTEM TABLE**************/
  // DML: Get system key
  public static final String STMT_SELECT_SYSTEM =
    "SELECT "
    + COLUMN_SQM_VALUE
    + " FROM " + TABLE_SQ_SYSTEM
    + " WHERE " + COLUMN_SQM_KEY + " = ?";

  //DML: Get deprecated or the new repo version system key
  public static final String STMT_SELECT_DEPRECATED_OR_NEW_SYSTEM_VERSION =
    "SELECT "
    + COLUMN_SQM_VALUE + " FROM " + TABLE_SQ_SYSTEM
    + " WHERE ( " + COLUMN_SQM_KEY + " = ? )"
    + " OR  (" + COLUMN_SQM_KEY + " = ? )";

  // DML: Remove system key
  public static final String STMT_DELETE_SYSTEM =
    "DELETE FROM "  + TABLE_SQ_SYSTEM
    + " WHERE " + COLUMN_SQM_KEY + " = ?";

  // DML: Insert new system key
  public static final String STMT_INSERT_SYSTEM =
    "INSERT INTO " + TABLE_SQ_SYSTEM + "("
    + COLUMN_SQM_KEY + ", "
    + COLUMN_SQM_VALUE + ") "
    + "VALUES(?, ?)";

  /*********CONFIGURABLE TABLE ***************/
  // DML: Select all connectors
  @Deprecated // used only for upgrade logic
  public static final String STMT_SELECT_CONNECTOR_ALL =
     "SELECT "
     + COLUMN_SQC_ID + ", "
     + COLUMN_SQC_NAME + ", "
     + COLUMN_SQC_CLASS + ", "
     + COLUMN_SQC_VERSION
     + " FROM " + TABLE_SQ_CONNECTOR;

   @Deprecated // used only in the upgrade path
   public static final String STMT_INSERT_INTO_CONNECTOR_WITHOUT_SUPPORTED_DIRECTIONS =
      "INSERT INTO " + TABLE_SQ_CONNECTOR+ " ("
          + COLUMN_SQC_NAME + ", "
          + COLUMN_SQC_CLASS + ", "
          + COLUMN_SQC_VERSION
          + ") VALUES (?, ?, ?)";

  //DML: Insert new connection
  @Deprecated // used only in upgrade path
  public static final String STMT_INSERT_CONNECTION =
    "INSERT INTO " + TABLE_SQ_CONNECTION + " ("
     + COLUMN_SQN_NAME + ", "
     + COLUMN_SQN_CONNECTOR + ","
     + COLUMN_SQN_ENABLED + ", "
     + COLUMN_SQN_CREATION_USER + ", "
     + COLUMN_SQN_CREATION_DATE + ", "
     + COLUMN_SQN_UPDATE_USER + ", " + COLUMN_SQN_UPDATE_DATE
     + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  /******* CONFIG and CONNECTOR DIRECTIONS ****/
  public static final String STMT_INSERT_DIRECTION = "INSERT INTO " + TABLE_SQ_DIRECTION + " "
       + "(" + COLUMN_SQD_NAME + ") VALUES (?)";

  public static final String STMT_FETCH_CONFIG_DIRECTIONS =
       "SELECT "
           + COLUMN_SQ_CFG_ID + ", "
           + COLUMN_SQ_CFG_DIRECTION
           + " FROM " + TABLE_SQ_CONFIG;

  private DerbySchemaInsertUpdateDeleteSelectQuery() {
    // Disable explicit object creation
  }
}