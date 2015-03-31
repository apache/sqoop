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

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.*;
import static org.apache.sqoop.repository.derby.DerbySchemaConstants.*;

// NOTE: If you have signed yourself to modify the schema for the repository
// such as a rename, change in table relationships or constraints, embrace yourself!
// The following code is supposed to be a chronological order of how the
// repository schema evolved. So do not ever change the following
// code directly. Instead make sure the upgrade queries are written to reflect
// the renames and changes in the table relationships or constraints
// It would have been nicer and much cleaner
// if this was not code but sql scripts. Having it in code it is very
// easy and tempting to rename or make changes to it and easily miss
// the upgrade code. Not to mention, make sure to
// the update the tests to use the upgrade queries as well
// Make sure to add a lot of comments to the upgrade code if there is an
// ordering dependency to help future contributors to not lose their sleep over
// enhancing this code
public final class DerbySchemaUpgradeQuery {

  // DDL: Increased size of SQ_CONNECTOR.SQC_VERSION to 64
  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTOR_MODIFY_COLUMN_SQC_VERSION_VARCHAR_64 =
    "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " ALTER COLUMN "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION) + " SET DATA TYPE VARCHAR(64)";

  // DDL: Add creation_user column to table SQ_SUBMISSION
  public static final String QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_CREATION_USER =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_USER) + " VARCHAR(32) "
      + "DEFAULT NULL";

  // DDL: Add update_user column to table SQ_SUBMISSION
  public static final String QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_UPDATE_USER =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_USER) + " VARCHAR(32) "
      + "DEFAULT NULL";

  //DDL: Add update_user column to table SQ_SUBMISSION
  public static final String QUERY_UPGRADE_TABLE_SQ_SUBMISSION_MODIFY_COLUMN_SQS_EXTERNAL_ID_VARCHAR_50 =
     "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + " ALTER COLUMN "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_ID) + " SET DATA TYPE VARCHAR(50)";

  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_ENABLED =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ENABLED) + " BOOLEAN "
      + "DEFAULT TRUE";

  // DDL: Add creation_user column to table SQ_CONNECTION
  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_CREATION_USER =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CREATION_USER) + " VARCHAR(32) "
      + "DEFAULT NULL";

  // DDL: Add update_user column to table SQ_CONNECTION
  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_UPDATE_USER =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_UPDATE_USER) + " VARCHAR(32) "
      + "DEFAULT NULL";
  // DDL: Add enabled column to table SQ_JOB
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_ENABLED =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ENABLED) + " BOOLEAN "
      + "DEFAULT TRUE";

  // DDL: Add creation_user column to table SQ_JOB
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_CREATION_USER =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CREATION_USER) + " VARCHAR(32) "
      + "DEFAULT NULL";

  // DDL: Add update_user column to table SQ_JOB
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_UPDATE_USER =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_USER) + " VARCHAR(32) "
      + "DEFAULT NULL";

  // Version 4 Upgrade
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_RENAME_COLUMN_SQB_CONNECTION_TO_SQB_FROM_CONNECTION =
      "RENAME COLUMN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CONNECTION)
        + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_CONNECTION);

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_SQB_TO_CONNECTION =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD COLUMN " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_CONNECTION)
        + " BIGINT";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_CONSTRAINT_SQB_SQN =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQN_NAME);

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_FROM =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQN_FROM_NAME)
          + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_CONNECTION) + ") REFERENCES "
          + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_TO =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQN_TO_NAME)
        + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_CONNECTION) + ") REFERENCES "
        + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_RENAME_COLUMN_SQF_OPERATION_TO_SQF_DIRECTION =
    "RENAME COLUMN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_OPERATION)
      + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION);

  //DML: Insert into form
  public static final String STMT_INSERT_INTO_FORM =
     "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME)+ " ("
     + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQF_TYPE) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQF_INDEX)
     + ") VALUES ( ?, ?, ?, ?)";

  // DML: Insert into inpu with form name
  public static final String STMT_INSERT_INTO_INPUT_WITH_FORM =
     "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " ("
     + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRMASK) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRLENGTH) + ", "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ENUMVALS)
     + ") VALUES (?, ?, ?, ?, ?, ?, ?)";


  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_OPERATION_TO_SQF_DIRECTION =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION)
        + "=? WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "=?"
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + " IS NOT NULL";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + "= ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + " IS NULL AND "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + " IN (?, ?)";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_NAME =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_DIRECTION =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "= ?"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION_COPY_SQB_FROM_CONNECTION =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " SET "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_CONNECTION) + "=" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_CONNECTION)
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TYPE) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_FROM_CONNECTION =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_CONNECTION) + "=?"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TYPE) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_CONNECTION) + "=?"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TYPE) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_NAME =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?"
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "= ?";

  // remove "input" from the prefix of the name for hdfs configs
  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_FROM_JOB_INPUT_NAMES =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + "=("
          + "? || SUBSTR(" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", 6) )"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + " IN ("
          + " SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID) + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?"
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "= ?)";

  // remove output from the prefix of the name for hdfs configs
  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_TO_JOB_INPUT_NAMES =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + "=("
          + "? || SUBSTR(" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", 7) )"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + " IN ("
          + " SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID) + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?"
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "= ?)";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_DIRECTION_TO_NULL =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SET "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "= NULL"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?";

   /** Intended for force driver creation and its related upgrades*/

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_NAME_FOR_DRIVER =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " SET "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + "= ?"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_CONFIGURABLE_ID_FOR_DRIVER =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " SET "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + "= ?"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_REMOVE_SECURITY_CONFIG_FOR_DRIVER =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_INPUT_REMOVE_SECURITY_CONFIG_INPUT_FOR_DRIVER =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_INPUT_UPDATE_CONFIG_INPUT_FOR_DRIVER =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + "= ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + "= ?";

  /**
   * Intended to change SQ_JOB_INPUT.SQBI_INPUT from EXPORT
   * throttling form, to IMPORT throttling form.
   */

  @Deprecated // only used for upgrade
  public static final String QUERY_SELECT_THROTTLING_FORM_INPUT_IDS =
      "SELECT SQI." + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " SQI"
          + " INNER JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SQF ON SQI." + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + "=SQF." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID)
          + " WHERE SQF." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "='throttling' AND SQF." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "=?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_INPUT_UPDATE_THROTTLING_FORM_INPUTS =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME) + " SQBI SET"
        + " SQBI." + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + "=(" + QUERY_SELECT_THROTTLING_FORM_INPUT_IDS
          + " AND SQI." + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + "=("
            + "SELECT SQI2." + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " SQI2"
            + " WHERE SQI2." + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + "=SQBI." + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + " FETCH FIRST 1 ROWS ONLY"
          +   "))"
        + "WHERE SQBI." + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + " IN (" + QUERY_SELECT_THROTTLING_FORM_INPUT_IDS + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_REMOVE_EXTRA_FORM_INPUTS =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " SQI"
        + " WHERE SQI." + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + " IN ("
          + "SELECT SQF." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID) + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SQF "
          + " WHERE SQF." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?"
          + " AND SQF." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "= ?)";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_REMOVE_EXTRA_DRIVER_FORM =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME)
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?"
        + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_DRIVER_INDEX =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " SET "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQF_INDEX) + "= ?"
        + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_COLUMN_SQB_TYPE =
      "ALTER TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " DROP COLUMN " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TYPE);

  // rename upgrades as part of the refactoring SQOOP-1498
  // table rename for CONNECTION-> LINK
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_1 = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_INPUT_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQNI_SQI_NAME);
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_2 = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_INPUT_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQNI_SQN_NAME);

  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_3 = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQN_FROM_NAME);

  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_4 = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQN_TO_NAME);

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_TO_SQ_LINK = "RENAME TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " TO SQ_LINK";

  // column only renames for SQ_CONNECTION
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_1 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_2 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_NAME) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_3 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CONNECTOR) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONNECTOR);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_4 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CREATION_USER) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_USER);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_5 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CREATION_DATE) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_DATE);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_6 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_UPDATE_USER) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_USER);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_7 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_UPDATE_DATE) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_DATE);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_8 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ENABLED) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ENABLED);

  // rename the constraint CONSTRAINT_SQF_SQC to CONSTRAINT_SQ_CFG_SQC
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQN_SQC_NAME);

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_LNK_SQC_NAME) + " " + "FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONNECTOR) + ") " + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID)
      + ")";

  // table rename for CONNECTION_INPUT -> LINK_INPUT
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_TO_SQ_LINK_INPUT = "RENAME TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_INPUT_NAME) + " TO SQ_LINK_INPUT";
  // column renames for SQ_CONNECTION_INPUT
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_1 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_CONNECTION) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_LINK);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_2 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_INPUT) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_INPUT);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_3 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_VALUE) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_VALUE);
  // add the dropped LINK table constraint to the LINK_INPUT
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_LINK_INPUT_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_LNKI_SQ_LNK_NAME) + " "
      + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_LINK) + ") " + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + ")";
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_LINK_INPUT_CONSTRAINT_2 = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_LNKI_SQI_NAME) + " "
      + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_INPUT) + ") " + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ")";

  // table rename for FORM-> CONFIG
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQI_SQF_NAME);

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_TO_SQ_CONFIG = "RENAME TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " TO SQ_CONFIG";

  // column and constraint renames for SQ_FORM
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_1 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_2 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONNECTOR);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_3 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_DIRECTION) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIRECTION);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_4 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_5 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_TYPE) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_TYPE);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_6 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQF_INDEX) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_INDEX);

  // rename the constraint CONSTRAINT_SQF_SQC to CONSTRAINT_SQ_CFG_SQC
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQF_SQC_NAME);

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_CFG_SQC_NAME)
      + " "
      + "FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONNECTOR)
      + ") "
      + "REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME)
      + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID)
      + ")";

  // column rename and constraint add for SQ_INPUT
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_INPUT_FORM_COLUMN = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG);

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_INPUT_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQI_SQ_CFG_NAME) + " " + "FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + ") REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID) + ")";

  // column rename and constraint add for SQ_JOB ( from and to link)
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_1 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_CONNECTION) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_LINK);
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_2 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_CONNECTION) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_LINK);

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_FROM = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQ_LNK_FROM_NAME) + " FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_LINK) + ") REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + ")";

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_TO = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQ_LNK_TO_NAME) + " FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_LINK) + ") REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_UNIQUE_CONSTRAINT_NAME = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_NAME_UNIQUE_NAME) + " UNIQUE ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_LINK_ADD_UNIQUE_CONSTRAINT_NAME = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_LNK_NAME_UNIQUE_NAME)
      + " UNIQUE ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + ")";

  // SQOOP-1557 upgrade queries for table rename for CONNECTOR-> CONFIGURABLE

  // drop the SQ_CONFIG FK for connector table
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_CFG_SQC_NAME);

  // drop the SQ_LINK FK for connector table
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_LINK_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_LNK_SQC_NAME);

  // drop the SQ_CONNECTOR_DIRECTION FK for connector table
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_DIRECTIONS_NAME) + " DROP CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQCD_SQC_NAME);

  // rename
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTOR_TO_SQ_CONFIGURABLE = "RENAME TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " TO SQ_CONFIGURABLE";

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONFIG_COLUMN_1 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONNECTOR) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE);

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_LINK_COLUMN_1 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONNECTOR) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE);

  // add a type column to the configurable
  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_COLUMN_SQC_TYPE = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME) + " ADD COLUMN " + CommonRepoUtils.escapeColumnName(COLUMN_SQC_TYPE) + " VARCHAR(32)";

  // add the constraints back for SQ_CONFIG
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONFIGURABLE_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_CFG_SQC_NAME)
      + " "
      + "FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE)
      + ") "
      + "REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
      + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")";

  // add the constraints back for SQ_LINK
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONFIGURABLE_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_LNK_SQC_NAME)
      + " "
      + "FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE)
      + ") "
      + "REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
      + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")";

  // add the constraints back for SQ_CONNECTOR_DIRECTION
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_DIRECTIONS_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQCD_SQC_NAME)
      + " "
      + "FOREIGN KEY ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_CONNECTOR)
      + ") "
      + "REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
      + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")";

 // add the constraints back for SQ_CONNECTOR_DIRECTION
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONNECTOR_DIRECTION_CONFIGURABLE_CONSTRAINT = "ALTER TABLE "
     + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQCD_SQC_NAME) + " "
       + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_CONNECTOR) + ") "
         + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")";

  // upgrade for config and connector-directions

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_DROP_COLUMN_SQ_CFG_DIRECTION_VARCHAR = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " DROP COLUMN " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIRECTION);

  // add unique constraint on the configurable table for name
  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_UNIQUE_CONSTRAINT_NAME = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_CONFIGURABLE_UNIQUE_NAME)
      + " UNIQUE (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + ")";

  // add unique constraint on the config table for name and type and configurableId
  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIGURABLE_ID = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_CONFIG_UNIQUE_NAME_TYPE_CONFIGURABLE)
      + " UNIQUE ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_TYPE) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + ")";

  // add unique constraint on the input table for name and type and configId
  public static final String QUERY_UPGRADE_TABLE_SQ_INPUT_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIG_ID = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
      + " ADD CONSTRAINT "
      + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_INPUT_UNIQUE_NAME_TYPE_CONFIG)
      + " UNIQUE ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + ")";

  // rename exception to error_summary
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_1 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXCEPTION) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_SUMMARY);

  //rename exception_trace to error_details
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_2 = "RENAME COLUMN "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + "." + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXCEPTION_TRACE) + " TO " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_DETAILS);

  // SQOOP-1804, column add for SQ_INPUT
  public static final String QUERY_UPGRADE_TABLE_SQ_INPUT_ADD_COLUMN_SQI_EDITABLE = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " ADD COLUMN "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_EDITABLE) + " VARCHAR(32)";

  // Add 1.99.3 constraints
  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQF_SQC = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQF_SQC_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQI_SQF = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQI_SQF_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQN_SQC = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQN_SQC_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CONNECTOR) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQB_SQN = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQN_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CONNECTION) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQNI_SQN = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_INPUT_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQNI_SQN_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_CONNECTION) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQNI_SQI = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_INPUT_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQNI_SQI_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_INPUT) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQBI_SQB = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQBI_SQB_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQBI_SQI = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQBI_SQI_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQS_SQB = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQS_SQB_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + ") ON DELETE CASCADE";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQRS_SQG = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_SUBMISSION_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQRS_SQG_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_GROUP) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_GROUP_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQG_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQRS_SQR = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_SUBMISSION_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQRS_SQR_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_COUNTER) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQR_ID) + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQRS_SQS = "ALTER TABLE "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_SUBMISSION_NAME)
      + " ADD CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQRS_SQS_NAME)
      + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_SUBMISSION) + ") REFERENCES "
      + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + ") ON DELETE CASCADE";

  public static final String getDropConstraintQuery(String schemaName, String tableName, String constraintName) {
    StringBuilder queryBuilder = new StringBuilder();

    queryBuilder.append("ALTER TABLE ");
    queryBuilder.append(CommonRepoUtils.getTableName(schemaName, tableName));
    queryBuilder.append(" DROP CONSTRAINT ");
    queryBuilder.append(CommonRepoUtils.getConstraintName(schemaName, constraintName));

    return queryBuilder.toString();
  }

  // Update Generic Jdbc Connector configs

  public static final String QUERY_UPDATE_TABLE_SQ_CONFIG_NAME =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
          + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + " = ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID) + " = ?";

  public static final String QUERY_UPDATE_TABLE_SQ_INPUT_SQI_NAME =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + " = ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + " = ?"
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + " = ?";

  public static final String QUERY_SELECT_CONFIG_ID_BY_NAME =
      "SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + " = ?";

  public static final String QUERY_SELECT_DIRECTION_CONFIG_BY_DIRECTION_NAME =
      "SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_CONFIG)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_DIRECTIONS_NAME)
          + " LEFT JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_DIRECTION_NAME)
            + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_DIRECTION)
            + " = " + CommonRepoUtils.escapeColumnName(COLUMN_SQD_ID)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQD_NAME) + " = ?";

  public static final String QUERY_SELECT_CONFIG_ID_BY_NAME_AND_DIRECTION = QUERY_SELECT_CONFIG_ID_BY_NAME
      + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID) + " IN ("
        + QUERY_SELECT_DIRECTION_CONFIG_BY_DIRECTION_NAME
      + ")";

  private DerbySchemaUpgradeQuery() {
    // Disable explicit object creation
  }
}