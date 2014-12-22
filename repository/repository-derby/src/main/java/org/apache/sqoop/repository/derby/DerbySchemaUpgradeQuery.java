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
    "ALTER TABLE " + TABLE_SQ_CONNECTOR + " ALTER COLUMN "
      + COLUMN_SQC_VERSION + " SET DATA TYPE VARCHAR(64)";

  // DDL: Add creation_user column to table SQ_SUBMISSION
  public static final String QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_CREATION_USER =
      "ALTER TABLE " + TABLE_SQ_SUBMISSION + " ADD "
      + COLUMN_SQS_CREATION_USER + " VARCHAR(32) "
      + "DEFAULT NULL";

  // DDL: Add update_user column to table SQ_SUBMISSION
  public static final String QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_UPDATE_USER =
      "ALTER TABLE " + TABLE_SQ_SUBMISSION + " ADD "
      + COLUMN_SQS_UPDATE_USER + " VARCHAR(32) "
      + "DEFAULT NULL";

  //DDL: Add update_user column to table SQ_SUBMISSION
  public static final String QUERY_UPGRADE_TABLE_SQ_SUBMISSION_MODIFY_COLUMN_SQS_EXTERNAL_ID_VARCHAR_50 =
     "ALTER TABLE " + TABLE_SQ_SUBMISSION + " ALTER COLUMN "
     + COLUMN_SQS_EXTERNAL_ID + " SET DATA TYPE VARCHAR(50)";

  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_ENABLED =
      "ALTER TABLE " + TABLE_SQ_CONNECTION + " ADD "
      + COLUMN_SQN_ENABLED + " BOOLEAN "
      + "DEFAULT TRUE";

  // DDL: Add creation_user column to table SQ_CONNECTION
  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_CREATION_USER =
      "ALTER TABLE " + TABLE_SQ_CONNECTION + " ADD "
      + COLUMN_SQN_CREATION_USER + " VARCHAR(32) "
      + "DEFAULT NULL";

  // DDL: Add update_user column to table SQ_CONNECTION
  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_UPDATE_USER =
      "ALTER TABLE " + TABLE_SQ_CONNECTION + " ADD "
      + COLUMN_SQN_UPDATE_USER + " VARCHAR(32) "
      + "DEFAULT NULL";
  // DDL: Add enabled column to table SQ_JOB
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_ENABLED =
      "ALTER TABLE " + TABLE_SQ_JOB + " ADD "
      + COLUMN_SQB_ENABLED + " BOOLEAN "
      + "DEFAULT TRUE";

  // DDL: Add creation_user column to table SQ_JOB
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_CREATION_USER =
      "ALTER TABLE " + TABLE_SQ_JOB + " ADD "
      + COLUMN_SQB_CREATION_USER + " VARCHAR(32) "
      + "DEFAULT NULL";

  // DDL: Add update_user column to table SQ_JOB
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_UPDATE_USER =
      "ALTER TABLE " + TABLE_SQ_JOB + " ADD "
      + COLUMN_SQB_UPDATE_USER + " VARCHAR(32) "
      + "DEFAULT NULL";

  // Version 4 Upgrade
  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_RENAME_COLUMN_SQB_CONNECTION_TO_SQB_FROM_CONNECTION =
      "RENAME COLUMN " + TABLE_SQ_JOB + "." + COLUMN_SQB_CONNECTION
        + " TO " + COLUMN_SQB_FROM_CONNECTION;

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_SQB_TO_CONNECTION =
      "ALTER TABLE " + TABLE_SQ_JOB + " ADD COLUMN " + COLUMN_SQB_TO_CONNECTION
        + " BIGINT";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_CONSTRAINT_SQB_SQN =
      "ALTER TABLE " + TABLE_SQ_JOB + " DROP CONSTRAINT " + CONSTRAINT_SQB_SQN;

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_FROM =
      "ALTER TABLE " + TABLE_SQ_JOB + " ADD CONSTRAINT " + CONSTRAINT_SQB_SQN_FROM
          + " FOREIGN KEY (" + COLUMN_SQB_FROM_CONNECTION + ") REFERENCES "
          + TABLE_SQ_CONNECTION + " (" + COLUMN_SQN_ID + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_TO =
      "ALTER TABLE " + TABLE_SQ_JOB + " ADD CONSTRAINT " + CONSTRAINT_SQB_SQN_TO
        + " FOREIGN KEY (" + COLUMN_SQB_TO_CONNECTION + ") REFERENCES "
        + TABLE_SQ_CONNECTION + " (" + COLUMN_SQN_ID + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_RENAME_COLUMN_SQF_OPERATION_TO_SQF_DIRECTION =
    "RENAME COLUMN " + TABLE_SQ_FORM + "." + COLUMN_SQF_OPERATION
      + " TO " + COLUMN_SQF_DIRECTION;

  //DML: Insert into form
  public static final String STMT_INSERT_INTO_FORM =
     "INSERT INTO " + TABLE_SQ_FORM+ " ("
     + COLUMN_SQF_CONNECTOR + ", "
     + COLUMN_SQF_NAME + ", "
     + COLUMN_SQF_TYPE + ", "
     + COLUMN_SQF_INDEX
     + ") VALUES ( ?, ?, ?, ?)";

  // DML: Insert into inpu with form name
  public static final String STMT_INSERT_INTO_INPUT_WITH_FORM =
     "INSERT INTO " + TABLE_SQ_INPUT + " ("
     + COLUMN_SQI_NAME + ", "
     + COLUMN_SQI_FORM + ", "
     + COLUMN_SQI_INDEX + ", "
     + COLUMN_SQI_TYPE + ", "
     + COLUMN_SQI_STRMASK + ", "
     + COLUMN_SQI_STRLENGTH + ", "
     + COLUMN_SQI_ENUMVALS
     + ") VALUES (?, ?, ?, ?, ?, ?, ?)";


  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_OPERATION_TO_SQF_DIRECTION =
      "UPDATE " + TABLE_SQ_FORM + " SET " + COLUMN_SQF_DIRECTION
        + "=? WHERE " + COLUMN_SQF_DIRECTION + "=?"
          + " AND " + COLUMN_SQF_CONNECTOR + " IS NOT NULL";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR =
      "UPDATE " + TABLE_SQ_FORM + " SET " + COLUMN_SQF_CONNECTOR + "= ?"
          + " WHERE " + COLUMN_SQF_CONNECTOR + " IS NULL AND "
          + COLUMN_SQF_NAME + " IN (?, ?)";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_NAME =
      "UPDATE " + TABLE_SQ_FORM + " SET " + COLUMN_SQF_NAME + "= ?"
        + " WHERE " + COLUMN_SQF_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_DIRECTION =
      "UPDATE " + TABLE_SQ_FORM + " SET " + COLUMN_SQF_DIRECTION + "= ?"
        + " WHERE " + COLUMN_SQF_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION_COPY_SQB_FROM_CONNECTION =
      "UPDATE " + TABLE_SQ_JOB + " SET "
        + COLUMN_SQB_TO_CONNECTION + "=" + COLUMN_SQB_FROM_CONNECTION
        + " WHERE " + COLUMN_SQB_TYPE + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_FROM_CONNECTION =
      "UPDATE " + TABLE_SQ_JOB + " SET " + COLUMN_SQB_FROM_CONNECTION + "=?"
        + " WHERE " + COLUMN_SQB_TYPE + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION =
      "UPDATE " + TABLE_SQ_JOB + " SET " + COLUMN_SQB_TO_CONNECTION + "=?"
        + " WHERE " + COLUMN_SQB_TYPE + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_NAME =
      "UPDATE " + TABLE_SQ_FORM + " SET "
          + COLUMN_SQF_NAME + "= ?"
          + " WHERE " + COLUMN_SQF_NAME + "= ?"
          + " AND " + COLUMN_SQF_DIRECTION + "= ?";

  // remove "input" from the prefix of the name for hdfs configs
  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_FROM_JOB_INPUT_NAMES =
      "UPDATE " + TABLE_SQ_INPUT + " SET "
          + COLUMN_SQI_NAME + "=("
          + "? || SUBSTR(" + COLUMN_SQI_NAME + ", 6) )"
          + " WHERE " + COLUMN_SQI_FORM + " IN ("
          + " SELECT " + COLUMN_SQF_ID + " FROM " + TABLE_SQ_FORM + " WHERE " + COLUMN_SQF_NAME + "= ?"
          + " AND " + COLUMN_SQF_DIRECTION + "= ?)";

  // remove output from the prefix of the name for hdfs configs
  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_TO_JOB_INPUT_NAMES =
      "UPDATE " + TABLE_SQ_INPUT + " SET "
          + COLUMN_SQI_NAME + "=("
          + "? || SUBSTR(" + COLUMN_SQI_NAME + ", 7) )"
          + " WHERE " + COLUMN_SQI_FORM + " IN ("
          + " SELECT " + COLUMN_SQF_ID + " FROM " + TABLE_SQ_FORM + " WHERE " + COLUMN_SQF_NAME + "= ?"
          + " AND " + COLUMN_SQF_DIRECTION + "= ?)";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_DIRECTION_TO_NULL =
      "UPDATE " + TABLE_SQ_FORM + " SET "
        + COLUMN_SQF_DIRECTION + "= NULL"
        + " WHERE " + COLUMN_SQF_NAME + "= ?";

   /** Intended for force driver creation and its related upgrades*/

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_NAME_FOR_DRIVER =
      "UPDATE " + TABLE_SQ_CONFIG + " SET "
        + COLUMN_SQ_CFG_NAME + "= ?"
        + " WHERE " + COLUMN_SQ_CFG_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_CONFIGURABLE_ID_FOR_DRIVER =
      "UPDATE " + TABLE_SQ_CONFIG + " SET "
        + COLUMN_SQ_CFG_CONFIGURABLE + "= ?"
        + " WHERE " + COLUMN_SQ_CFG_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_REMOVE_SECURITY_CONFIG_FOR_DRIVER =
      "DELETE FROM " + TABLE_SQ_CONFIG
        + " WHERE " + COLUMN_SQ_CFG_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_INPUT_REMOVE_SECURITY_CONFIG_INPUT_FOR_DRIVER =
      "DELETE FROM " + TABLE_SQ_INPUT
        + " WHERE " + COLUMN_SQI_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_INPUT_UPDATE_CONFIG_INPUT_FOR_DRIVER =
      "UPDATE " + TABLE_SQ_INPUT + " SET "
          + COLUMN_SQI_NAME + "= ?"
          + " WHERE " + COLUMN_SQI_NAME + "= ?";

  /**
   * Intended to change SQ_JOB_INPUT.SQBI_INPUT from EXPORT
   * throttling form, to IMPORT throttling form.
   */

  @Deprecated // only used for upgrade
  public static final String QUERY_SELECT_THROTTLING_FORM_INPUT_IDS =
      "SELECT SQI." + COLUMN_SQI_ID + " FROM " + TABLE_SQ_INPUT + " SQI"
          + " INNER JOIN " + TABLE_SQ_FORM + " SQF ON SQI." + COLUMN_SQI_FORM + "=SQF." + COLUMN_SQF_ID
          + " WHERE SQF." + COLUMN_SQF_NAME + "='throttling' AND SQF." + COLUMN_SQF_DIRECTION + "=?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_INPUT_UPDATE_THROTTLING_FORM_INPUTS =
      "UPDATE " + TABLE_SQ_JOB_INPUT + " SQBI SET"
        + " SQBI." + COLUMN_SQBI_INPUT + "=(" + QUERY_SELECT_THROTTLING_FORM_INPUT_IDS
          + " AND SQI." + COLUMN_SQI_NAME + "=("
            + "SELECT SQI2." + COLUMN_SQI_NAME + " FROM " + TABLE_SQ_INPUT + " SQI2"
            + " WHERE SQI2." + COLUMN_SQI_ID + "=SQBI." + COLUMN_SQBI_INPUT + " FETCH FIRST 1 ROWS ONLY"
          +   "))"
        + "WHERE SQBI." + COLUMN_SQBI_INPUT + " IN (" + QUERY_SELECT_THROTTLING_FORM_INPUT_IDS + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_REMOVE_EXTRA_FORM_INPUTS =
      "DELETE FROM " + TABLE_SQ_INPUT + " SQI"
        + " WHERE SQI." + COLUMN_SQI_FORM + " IN ("
          + "SELECT SQF." + COLUMN_SQF_ID + " FROM " + TABLE_SQ_FORM + " SQF "
          + " WHERE SQF." + COLUMN_SQF_NAME + "= ?"
          + " AND SQF." + COLUMN_SQF_DIRECTION + "= ?)";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_REMOVE_EXTRA_DRIVER_FORM =
      "DELETE FROM " + TABLE_SQ_FORM
        + " WHERE " + COLUMN_SQF_NAME + "= ?"
        + " AND " + COLUMN_SQF_DIRECTION + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_DRIVER_INDEX =
      "UPDATE " + TABLE_SQ_FORM + " SET "
        + COLUMN_SQF_INDEX + "= ?"
        + " WHERE " + COLUMN_SQF_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_COLUMN_SQB_TYPE =
      "ALTER TABLE " + TABLE_SQ_JOB + " DROP COLUMN " + COLUMN_SQB_TYPE;

  // rename upgrades as part of the refactoring SQOOP-1498
  // table rename for CONNECTION-> LINK
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_1 = "ALTER TABLE "
      + TABLE_SQ_CONNECTION_INPUT + " DROP CONSTRAINT " + CONSTRAINT_SQNI_SQI;
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_2 = "ALTER TABLE "
      + TABLE_SQ_CONNECTION_INPUT + " DROP CONSTRAINT " + CONSTRAINT_SQNI_SQN;

  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_3 = "ALTER TABLE "
      + TABLE_SQ_JOB + " DROP CONSTRAINT " + CONSTRAINT_SQB_SQN_FROM;

  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_4 = "ALTER TABLE "
      + TABLE_SQ_JOB + " DROP CONSTRAINT " + CONSTRAINT_SQB_SQN_TO;

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_TO_SQ_LINK = "RENAME TABLE "
      + TABLE_SQ_CONNECTION + " TO SQ_LINK";

  // column only renames for SQ_CONNECTION
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_1 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_ID + " TO " + COLUMN_SQ_LNK_ID;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_2 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_NAME + " TO " + COLUMN_SQ_LNK_NAME;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_3 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_CONNECTOR + " TO " + COLUMN_SQ_LNK_CONNECTOR;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_4 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_CREATION_USER + " TO " + COLUMN_SQ_LNK_CREATION_USER;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_5 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_CREATION_DATE + " TO " + COLUMN_SQ_LNK_CREATION_DATE;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_6 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_UPDATE_USER + " TO " + COLUMN_SQ_LNK_UPDATE_USER;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_7 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_UPDATE_DATE + " TO " + COLUMN_SQ_LNK_UPDATE_DATE;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_8 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQN_ENABLED + " TO " + COLUMN_SQ_LNK_ENABLED;

  // rename the constraint CONSTRAINT_SQF_SQC to CONSTRAINT_SQ_CFG_SQC
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_LINK + " DROP CONSTRAINT " + CONSTRAINT_SQN_SQC;

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_LINK + " ADD CONSTRAINT " + CONSTRAINT_SQ_LNK_SQC + " " + "FOREIGN KEY ("
      + COLUMN_SQ_LNK_CONNECTOR + ") " + "REFERENCES " + TABLE_SQ_CONNECTOR + " (" + COLUMN_SQC_ID
      + ")";

  // table rename for CONNECTION_INPUT -> LINK_INPUT
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_TO_SQ_LINK_INPUT = "RENAME TABLE "
      + TABLE_SQ_CONNECTION_INPUT + " TO SQ_LINK_INPUT";
  // column renames for SQ_CONNECTION_INPUT
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_1 = "RENAME COLUMN "
      + TABLE_SQ_LINK_INPUT + "." + COLUMN_SQNI_CONNECTION + " TO " + COLUMN_SQ_LNKI_LINK;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_2 = "RENAME COLUMN "
      + TABLE_SQ_LINK_INPUT + "." + COLUMN_SQNI_INPUT + " TO " + COLUMN_SQ_LNKI_INPUT;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_3 = "RENAME COLUMN "
      + TABLE_SQ_LINK_INPUT + "." + COLUMN_SQNI_VALUE + " TO " + COLUMN_SQ_LNKI_VALUE;
  // add the dropped LINK table constraint to the LINK_INPUT
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_LINK_INPUT_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_LINK_INPUT + " ADD CONSTRAINT " + CONSTRAINT_SQ_LNKI_SQ_LNK + " "
      + "FOREIGN KEY (" + COLUMN_SQ_LNKI_LINK + ") " + "REFERENCES " + TABLE_SQ_LINK + " ("
      + COLUMN_SQ_LNK_ID + ")";

  // table rename for FORM-> CONFIG
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_INPUT + " DROP CONSTRAINT " + CONSTRAINT_SQI_SQF;

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_TO_SQ_CONFIG = "RENAME TABLE "
      + TABLE_SQ_FORM + " TO SQ_CONFIG";

  // column and constraint renames for SQ_FORM
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_1 = "RENAME COLUMN "
      + TABLE_SQ_CONFIG + "." + COLUMN_SQF_ID + " TO " + COLUMN_SQ_CFG_ID;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_2 = "RENAME COLUMN "
      + TABLE_SQ_CONFIG + "." + COLUMN_SQF_CONNECTOR + " TO " + COLUMN_SQ_CFG_CONNECTOR;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_3 = "RENAME COLUMN "
      + TABLE_SQ_CONFIG + "." + COLUMN_SQF_DIRECTION + " TO " + COLUMN_SQ_CFG_DIRECTION;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_4 = "RENAME COLUMN "
      + TABLE_SQ_CONFIG + "." + COLUMN_SQF_NAME + " TO " + COLUMN_SQ_CFG_NAME;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_5 = "RENAME COLUMN "
      + TABLE_SQ_CONFIG + "." + COLUMN_SQF_TYPE + " TO " + COLUMN_SQ_CFG_TYPE;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_6 = "RENAME COLUMN "
      + TABLE_SQ_CONFIG + "." + COLUMN_SQF_INDEX + " TO " + COLUMN_SQ_CFG_INDEX;

  // rename the constraint CONSTRAINT_SQF_SQC to CONSTRAINT_SQ_CFG_SQC
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_CONFIG + " DROP CONSTRAINT " + CONSTRAINT_SQF_SQC;

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_CONFIG
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQ_CFG_SQC
      + " "
      + "FOREIGN KEY ("
      + COLUMN_SQ_CFG_CONNECTOR
      + ") "
      + "REFERENCES "
      + TABLE_SQ_CONNECTOR
      + " ("
      + COLUMN_SQC_ID
      + ")";

  // column rename and constraint add for SQ_INPUT
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_INPUT_FORM_COLUMN = "RENAME COLUMN "
      + TABLE_SQ_INPUT + "." + COLUMN_SQI_FORM + " TO " + COLUMN_SQI_CONFIG;

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_INPUT_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_INPUT + " ADD CONSTRAINT " + CONSTRAINT_SQI_SQ_CFG + " " + "FOREIGN KEY ("
      + COLUMN_SQI_CONFIG + ") " + "REFERENCES " + TABLE_SQ_CONFIG + " (" + COLUMN_SQ_CFG_ID + ")";

  // column rename and constraint add for SQ_JOB ( from and to link)
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_1 = "RENAME COLUMN "
      + TABLE_SQ_JOB + "." + COLUMN_SQB_FROM_CONNECTION + " TO " + COLUMN_SQB_FROM_LINK;
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_2 = "RENAME COLUMN "
      + TABLE_SQ_JOB + "." + COLUMN_SQB_TO_CONNECTION + " TO " + COLUMN_SQB_TO_LINK;

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_FROM = "ALTER TABLE "
      + TABLE_SQ_JOB + " ADD CONSTRAINT " + CONSTRAINT_SQB_SQ_LNK_FROM + " FOREIGN KEY ("
      + COLUMN_SQB_FROM_LINK + ") REFERENCES " + TABLE_SQ_LINK + " (" + COLUMN_SQ_LNK_ID + ")";

  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_TO = "ALTER TABLE "
      + TABLE_SQ_JOB + " ADD CONSTRAINT " + CONSTRAINT_SQB_SQ_LNK_TO + " FOREIGN KEY ("
      + COLUMN_SQB_TO_LINK + ") REFERENCES " + TABLE_SQ_LINK + " (" + COLUMN_SQ_LNK_ID + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_ADD_UNIQUE_CONSTRAINT_NAME = "ALTER TABLE "
      + TABLE_SQ_JOB + " ADD CONSTRAINT " + CONSTRAINT_SQB_NAME_UNIQUE + " UNIQUE ("
      + COLUMN_SQB_NAME + ")";

  public static final String QUERY_UPGRADE_TABLE_SQ_LINK_ADD_UNIQUE_CONSTRAINT_NAME = "ALTER TABLE "
      + TABLE_SQ_LINK
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQ_LNK_NAME_UNIQUE
      + " UNIQUE ("
      + COLUMN_SQ_LNK_NAME + ")";

  // SQOOP-1557 upgrade queries for table rename for CONNECTOR-> CONFIGURABLE

  // drop the SQ_CONFIG FK for connector table
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_CONFIG + " DROP CONSTRAINT " + CONSTRAINT_SQ_CFG_SQC;

  // drop the SQ_LINK FK for connector table
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_LINK_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_LINK + " DROP CONSTRAINT " + CONSTRAINT_SQ_LNK_SQC;

  // drop the SQ_CONNECTOR_DIRECTION FK for connector table
  public static final String QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_CONNECTOR_DIRECTIONS + " DROP CONSTRAINT " + CONSTRAINT_SQCD_SQC;

  // rename
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTOR_TO_SQ_CONFIGURABLE = "RENAME TABLE "
      + TABLE_SQ_CONNECTOR + " TO SQ_CONFIGURABLE";

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_CONFIG_COLUMN_1 = "RENAME COLUMN "
      + TABLE_SQ_CONFIG + "." + COLUMN_SQ_CFG_CONNECTOR + " TO " + COLUMN_SQ_CFG_CONFIGURABLE;

  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_LINK_COLUMN_1 = "RENAME COLUMN "
      + TABLE_SQ_LINK + "." + COLUMN_SQ_LNK_CONNECTOR + " TO " + COLUMN_SQ_LNK_CONFIGURABLE;

  // add a type column to the configurable
  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_COLUMN_SQC_TYPE = "ALTER TABLE "
      + TABLE_SQ_CONFIGURABLE + " ADD COLUMN " + COLUMN_SQC_TYPE + " VARCHAR(32)";

  // add the constraints back for SQ_CONFIG
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONFIGURABLE_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_CONFIG
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQ_CFG_SQC
      + " "
      + "FOREIGN KEY ("
      + COLUMN_SQ_CFG_CONFIGURABLE
      + ") "
      + "REFERENCES "
      + TABLE_SQ_CONFIGURABLE
      + " ("
      + COLUMN_SQC_ID + ")";

  // add the constraints back for SQ_LINK
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONFIGURABLE_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_LINK
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQ_LNK_SQC
      + " "
      + "FOREIGN KEY ("
      + COLUMN_SQ_LNK_CONFIGURABLE
      + ") "
      + "REFERENCES "
      + TABLE_SQ_CONFIGURABLE
      + " ("
      + COLUMN_SQC_ID + ")";

  // add the constraints back for SQ_CONNECTOR_DIRECTION
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT = "ALTER TABLE "
      + TABLE_SQ_CONNECTOR_DIRECTIONS
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQCD_SQC
      + " "
      + "FOREIGN KEY ("
      + COLUMN_SQCD_CONNECTOR
      + ") "
      + "REFERENCES "
      + TABLE_SQ_CONFIGURABLE
      + " (" + COLUMN_SQC_ID + ")";

 // add the constraints back for SQ_CONNECTOR_DIRECTION
  public static final String QUERY_UPGRADE_ADD_TABLE_SQ_CONNECTOR_DIRECTION_CONFIGURABLE_CONSTRAINT = "ALTER TABLE "
     + TABLE_SQ_LINK + " ADD CONSTRAINT " + CONSTRAINT_SQCD_SQC + " "
       + "FOREIGN KEY (" + COLUMN_SQCD_CONNECTOR + ") "
         + "REFERENCES " + TABLE_SQ_CONFIGURABLE + " (" + COLUMN_SQC_ID + ")";

  // upgrade for config and connector-directions

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_DROP_COLUMN_SQ_CFG_DIRECTION_VARCHAR = "ALTER TABLE "
      + TABLE_SQ_CONFIG + " DROP COLUMN " + COLUMN_SQ_CFG_DIRECTION;

  // add unique constraint on the configurable table for name
  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_UNIQUE_CONSTRAINT_NAME = "ALTER TABLE "
      + TABLE_SQ_CONFIGURABLE
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQ_CONFIGURABLE_UNIQUE
      + " UNIQUE (" + COLUMN_SQC_NAME + ")";

  // add unique constraint on the config table for name and type and configurableId
  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIGURABLE_ID = "ALTER TABLE "
      + TABLE_SQ_CONFIG
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQ_CONFIG_UNIQUE
      + " UNIQUE ("
      + COLUMN_SQ_CFG_NAME + ", " + COLUMN_SQ_CFG_TYPE + ", " + COLUMN_SQ_CFG_CONFIGURABLE + ")";

  // add unique constraint on the input table for name and type and configId
  public static final String QUERY_UPGRADE_TABLE_SQ_INPUT_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIG_ID = "ALTER TABLE "
      + TABLE_SQ_INPUT
      + " ADD CONSTRAINT "
      + CONSTRAINT_SQ_INPUT_UNIQUE
      + " UNIQUE ("
      + COLUMN_SQI_NAME + ", " + COLUMN_SQI_TYPE + ", " + COLUMN_SQI_CONFIG + ")";

  // rename exception to error_summary
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_1 = "RENAME COLUMN "
      + TABLE_SQ_SUBMISSION + "." + COLUMN_SQS_EXCEPTION + " TO " + COLUMN_SQS_ERROR_SUMMARY;

  //rename exception_trace to error_details
  public static final String QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_2 = "RENAME COLUMN "
      + TABLE_SQ_SUBMISSION + "." + COLUMN_SQS_EXCEPTION_TRACE + " TO " + COLUMN_SQS_ERROR_DETAILS;

  private DerbySchemaUpgradeQuery() {
    // Disable explicit object creation
  }
}