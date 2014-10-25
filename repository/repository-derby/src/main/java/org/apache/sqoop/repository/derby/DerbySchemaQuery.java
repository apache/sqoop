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

import static org.apache.sqoop.repository.derby.DerbySchemaConstants.*;

/**
 * DDL queries that create the Sqoop repository schema in Derby database. These
 * queries create the following tables:
 * <p>
 * <strong>SQ_SYSTEM</strong>: Store for various state information
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_SYSTEM                  |
 *    +----------------------------+
 *    | SQM_ID: BIGINT PK          |
 *    | SQM_KEY: VARCHAR(64)       |
 *    | SQM_VALUE: VARCHAR(64)     |
 *    +----------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_DIRECTION</strong>: Directions.
 * <pre>
 *    +---------------------------------------+
 *    | SQ_DIRECTION                          |
 *    +---------------------------------------+
 *    | SQD_ID: BIGINT PK AUTO-GEN            |
 *    | SQD_NAME: VARCHAR(64)                 |"FROM"|"TO"
 *    +---------------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONFIGURABLE</strong>: Configurable registration.
 *
 * <pre>
 *    +-----------------------------+
 *    | SQ_CONFIGURABLE             |
 *    +-----------------------------+
 *    | SQC_ID: BIGINT PK AUTO-GEN  |
 *    | SQC_NAME: VARCHAR(64)       |
 *    | SQC_CLASS: VARCHAR(255)     |
 *    | SQC_TYPE: VARCHAR(32)       |"CONNECTOR"|"DRIVER"
 *    | SQC_VERSION: VARCHAR(64)    |
 *    +-----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONNECTOR_DIRECTIONS</strong>: Connector directions.
 * <pre>
 *    +------------------------------+
 *    | SQ_CONNECTOR_DIRECTIONS      |
 *    +------------------------------+
 *    | SQCD_ID: BIGINT PK AUTO-GEN  |
 *    | SQCD_CONNECTOR: BIGINT       |FK SQCD_CONNECTOR(SQC_ID)
 *    | SQCD_DIRECTION: BIGINT       |FK SQCD_DIRECTION(SQD_ID)
 *    +------------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_CONFIG</strong>: Config details.
 *
 * <pre>
 *    +-------------------------------------+
 *    | SQ_CONFIG                           |
 *    +-------------------------------------+
 *    | SQ_CFG_ID: BIGINT PK AUTO-GEN       |
 *    | SQ_CFG_CONNECTOR: BIGINT            |FK SQ_CFG_CONNECTOR(SQC_ID),NULL for driver
 *    | SQ_CFG_NAME: VARCHAR(64)            |
 *    | SQ_CFG_TYPE: VARCHAR(32)            |"LINK"|"JOB"
 *    | SQ_CFG_INDEX: SMALLINT              |
 *    +-------------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONFIG_DIRECTIONS</strong>: Connector directions.
 * <pre>
 *    +------------------------------+
 *    | SQ_CONNECTOR_DIRECTIONS      |
 *    +------------------------------+
 *    | SQCD_ID: BIGINT PK AUTO-GEN  |
 *    | SQCD_CONFIG: BIGINT          |FK SQCD_CONFIG(SQ_CFG_ID)
 *    | SQCD_DIRECTION: BIGINT       |FK SQCD_DIRECTION(SQD_ID)
 *    +------------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_INPUT</strong>: Input details
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_INPUT                   |
 *    +----------------------------+
 *    | SQI_ID: BIGINT PK AUTO-GEN |
 *    | SQI_NAME: VARCHAR(64)      |
 *    | SQI_CONFIG: BIGINT         |FK SQ_CONFIG(SQ_CFG_ID)
 *    | SQI_INDEX: SMALLINT        |
 *    | SQI_TYPE: VARCHAR(32)      |"STRING"|"MAP"
 *    | SQI_STRMASK: BOOLEAN       |
 *    | SQI_STRLENGTH: SMALLINT    |
 *    | SQI_ENUMVALS: VARCHAR(100) |
 *    +----------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_LINK</strong>: Stored links
 *
 * <pre>
 *    +--------------------------------+
 *    | SQ_LINK                  |
 *    +--------------------------------+
 *    | SQ_LNK_ID: BIGINT PK AUTO-GEN     |
 *    | SQ_LNK_NAME: VARCHAR(64)          |
 *    | SQ_LNK_CONNECTOR: BIGINT          | FK SQ_CONNECTOR(SQC_ID)
 *    | SQ_LNK_CREATION_USER: VARCHAR(32) |
 *    | SQ_LNK_CREATION_DATE: TIMESTAMP   |
 *    | SQ_LNK_UPDATE_USER: VARCHAR(32)   |
 *    | SQ_LNK_UPDATE_DATE: TIMESTAMP     |
 *    | SQ_LNK_ENABLED: BOOLEAN           |
 *    +--------------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_JOB</strong>: Stored jobs
 *
 * <pre>
 *    +--------------------------------+
 *    | SQ_JOB                         |
 *    +--------------------------------+
 *    | SQB_ID: BIGINT PK AUTO-GEN     |
 *    | SQB_NAME: VARCHAR(64)          |
 *    | SQB_FROM_LINK: BIGINT          |FK SQ_LINK(SQ_LNK_ID)
 *    | SQB_TO_LINK: BIGINT            |FK SQ_LINK(SQ_LNK_ID)
 *    | SQB_CREATION_USER: VARCHAR(32) |
 *    | SQB_CREATION_DATE: TIMESTAMP   |
 *    | SQB_UPDATE_USER: VARCHAR(32)   |
 *    | SQB_UPDATE_DATE: TIMESTAMP     |
 *    | SQB_ENABLED: BOOLEAN           |
 *    +--------------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_LINK_INPUT</strong>: N:M relationship link and input
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_LINK_INPUT              |
 *    +----------------------------+
 *    | SQ_LNKI_LINK: BIGINT PK    | FK SQ_LINK(SQ_LNK_ID)
 *    | SQ_LNKI_INPUT: BIGINT PK   | FK SQ_INPUT(SQI_ID)
 *    | SQ_LNKI_VALUE: LONG VARCHAR|
 *    +----------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_JOB_INPUT</strong>: N:M relationship job and input
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_JOB_INPUT               |
 *    +----------------------------+
 *    | SQBI_JOB: BIGINT PK        | FK SQ_JOB(SQB_ID)
 *    | SQBI_INPUT: BIGINT PK      | FK SQ_INPUT(SQI_ID)
 *    | SQBI_VALUE: LONG VARCHAR   |
 *    +----------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_SUBMISSION</strong>: List of submissions
 *
 * <pre>
 *    +-----------------------------------+
 *    | SQ_JOB_SUBMISSION                 |
 *    +-----------------------------------+
 *    | SQS_ID: BIGINT PK                 |
 *    | SQS_JOB: BIGINT                   | FK SQ_JOB(SQB_ID)
 *    | SQS_STATUS: VARCHAR(20)           |
 *    | SQS_CREATION_USER: VARCHAR(32)    |
 *    | SQS_CREATION_DATE: TIMESTAMP      |
 *    | SQS_UPDATE_USER: VARCHAR(32)      |
 *    | SQS_UPDATE_DATE: TIMESTAMP        |
 *    | SQS_EXTERNAL_ID: VARCHAR(50)      |
 *    | SQS_EXTERNAL_LINK: VARCHAR(150)   |
 *    | SQS_EXCEPTION: VARCHAR(150)       |
 *    | SQS_EXCEPTION_TRACE: VARCHAR(750) |
 *    +-----------------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_COUNTER_GROUP</strong>: List of counter groups
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_COUNTER_GROUP           |
 *    +----------------------------+
 *    | SQG_ID: BIGINT PK          |
 *    | SQG_NAME: VARCHAR(75)      |
 *    +----------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_COUNTER</strong>: List of counters
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_COUNTER                 |
 *    +----------------------------+
 *    | SQR_ID: BIGINT PK          |
 *    | SQR_NAME: VARCHAR(75)      |
 *    +----------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_COUNTER_SUBMISSION</strong>: N:M Relationship
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_COUNTER_SUBMISSION      |
 *    +----------------------------+
 *    | SQRS_GROUP: BIGINT PK      | FK SQ_COUNTER_GROUP(SQR_ID)
 *    | SQRS_COUNTER: BIGINT PK    | FK SQ_COUNTER(SQR_ID)
 *    | SQRS_SUBMISSION: BIGINT PK | FK SQ_SUBMISSION(SQS_ID)
 *    | SQRS_VALUE: BIGINT         |
 *    +----------------------------+
 * </pre>
 *
 * </p>
 */
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
public final class DerbySchemaQuery {

  // DDL: Create schema
  public static final String QUERY_CREATE_SCHEMA_SQOOP =
   "CREATE SCHEMA " + SCHEMA_SQOOP;

  public static final String QUERY_SYSSCHEMA_SQOOP =
   "SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '"
   + SCHEMA_SQOOP + "'";

  // DDL: Create table SQ_SYSTEM
  public static final String QUERY_CREATE_TABLE_SQ_SYSTEM =
    "CREATE TABLE " + TABLE_SQ_SYSTEM + " ("
    + COLUMN_SQM_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
    + COLUMN_SQM_KEY + " VARCHAR(64), "
    + COLUMN_SQM_VALUE + " VARCHAR(64) "
    + ")";

  // DDL: Create table SQ_DIRECTION
  public static final String QUERY_CREATE_TABLE_SQ_DIRECTION =
   "CREATE TABLE " + TABLE_SQ_DIRECTION + " ("
   + COLUMN_SQD_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
   + COLUMN_SQD_NAME + " VARCHAR(64)"
   + ")";

  // DDL: Create table SQ_CONNECTOR
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR =
      "CREATE TABLE " + TABLE_SQ_CONNECTOR + " ("
      + COLUMN_SQC_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + COLUMN_SQC_NAME + " VARCHAR(64), "
      + COLUMN_SQC_CLASS + " VARCHAR(255), "
      + COLUMN_SQC_VERSION + " VARCHAR(64) "
      + ")";

  // DDL: Create table SQ_CONNECTOR_DIRECTIONS
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS =
      "CREATE TABLE " + TABLE_SQ_CONNECTOR_DIRECTIONS + " ("
      + COLUMN_SQCD_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + COLUMN_SQCD_CONNECTOR + " BIGINT, "
      + COLUMN_SQCD_DIRECTION + " BIGINT, "
      + "CONSTRAINT " + CONSTRAINT_SQCD_SQC + " "
        + "FOREIGN KEY (" + COLUMN_SQCD_CONNECTOR + ") "
          + "REFERENCES " + TABLE_SQ_CONNECTOR + " (" + COLUMN_SQC_ID + "), "
      + "CONSTRAINT " + CONSTRAINT_SQCD_SQD + " "
        + "FOREIGN KEY (" + COLUMN_SQCD_DIRECTION + ") "
          + "REFERENCES " + TABLE_SQ_DIRECTION + " (" + COLUMN_SQD_ID + ")"
      + ")";

   // DDL: Create table SQ_FORM
  public static final String QUERY_CREATE_TABLE_SQ_FORM =
      "CREATE TABLE " + TABLE_SQ_FORM + " ("
      + COLUMN_SQF_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + COLUMN_SQF_CONNECTOR + " BIGINT, "
      + COLUMN_SQF_OPERATION + " VARCHAR(32), "
      + COLUMN_SQF_NAME + " VARCHAR(64), "
      + COLUMN_SQF_TYPE + " VARCHAR(32), "
      + COLUMN_SQF_INDEX + " SMALLINT, "
      + "CONSTRAINT " + CONSTRAINT_SQF_SQC + " "
        + "FOREIGN KEY (" + COLUMN_SQF_CONNECTOR + ") "
          + "REFERENCES " + TABLE_SQ_CONNECTOR + " (" + COLUMN_SQC_ID + ")"
      + ")";

  // DDL: Create table SQ_CONFIG_DIRECTIONS ( same as SQ_FORM_DIRECTIONS)
  // Note: that the form was renamed to config at one point and this code was added after the rename
  // DDL: Create table SQ_CONFIG_DIRECTIONS
 public static final String QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS =
     "CREATE TABLE " + TABLE_SQ_CONFIG_DIRECTIONS + " ("
     + COLUMN_SQ_CFG_DIR_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
     + COLUMN_SQ_CFG_DIR_CONFIG + " BIGINT, "
     + COLUMN_SQ_CFG_DIR_DIRECTION + " BIGINT, "
     + "CONSTRAINT " + CONSTRAINT_SQ_CFG_DIR_CONFIG + " "
       + "FOREIGN KEY (" + COLUMN_SQ_CFG_DIR_CONFIG + ") "
         + "REFERENCES " + TABLE_SQ_CONFIG + " (" + COLUMN_SQ_CFG_ID + "), "
     + "CONSTRAINT " + CONSTRAINT_SQ_CFG_DIR_DIRECTION + " "
       + "FOREIGN KEY (" + COLUMN_SQ_CFG_DIR_DIRECTION + ") "
         + "REFERENCES " + TABLE_SQ_DIRECTION + " (" + COLUMN_SQD_ID + ")"
     + ")";


  // DDL: Create table SQ_INPUT
  public static final String QUERY_CREATE_TABLE_SQ_INPUT =
      "CREATE TABLE " + TABLE_SQ_INPUT + " ("
      + COLUMN_SQI_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + COLUMN_SQI_NAME + " VARCHAR(64), "
      + COLUMN_SQI_FORM + " BIGINT, "
      + COLUMN_SQI_INDEX + " SMALLINT, "
      + COLUMN_SQI_TYPE + " VARCHAR(32), "
      + COLUMN_SQI_STRMASK + " BOOLEAN, "
      + COLUMN_SQI_STRLENGTH + " SMALLINT, "
      + COLUMN_SQI_ENUMVALS + " VARCHAR(100),"
      + "CONSTRAINT " + CONSTRAINT_SQI_SQF + " "
        + "FOREIGN KEY (" + COLUMN_SQI_FORM + ") "
          + "REFERENCES " + TABLE_SQ_FORM + " (" + COLUMN_SQF_ID + ")"
      + ")";

  // DDL: Create table SQ_CONNECTION
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTION =
      "CREATE TABLE " + TABLE_SQ_CONNECTION + " ("
      + COLUMN_SQN_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + COLUMN_SQN_CONNECTOR + " BIGINT, "
      + COLUMN_SQN_NAME  + " VARCHAR(32),"
      + COLUMN_SQN_CREATION_DATE + " TIMESTAMP,"
      + COLUMN_SQN_UPDATE_DATE + " TIMESTAMP,"
      + "CONSTRAINT " + CONSTRAINT_SQN_SQC + " "
        + "FOREIGN KEY(" + COLUMN_SQN_CONNECTOR + ") "
          + " REFERENCES " + TABLE_SQ_CONNECTOR + " (" + COLUMN_SQC_ID + ")"
      + ")";

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

//DDL: Create table SQ_JOB
 public static final String QUERY_CREATE_TABLE_SQ_JOB =
     "CREATE TABLE " + TABLE_SQ_JOB + " ("
     + COLUMN_SQB_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
     + COLUMN_SQB_CONNECTION + " BIGINT, "
     + COLUMN_SQB_NAME + " VARCHAR(64), "
     + COLUMN_SQB_TYPE + " VARCHAR(64),"
     + COLUMN_SQB_CREATION_DATE + " TIMESTAMP,"
     + COLUMN_SQB_UPDATE_DATE + " TIMESTAMP,"
     + "CONSTRAINT " + CONSTRAINT_SQB_SQN + " "
       + "FOREIGN KEY(" + COLUMN_SQB_CONNECTION + ") "
         + "REFERENCES " + TABLE_SQ_CONNECTION + " (" + COLUMN_SQN_ID + ")"
     + ")";


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

  // DDL: Create table SQ_CONNECTION_INPUT
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTION_INPUT =
      "CREATE TABLE " + TABLE_SQ_CONNECTION_INPUT + " ("
      + COLUMN_SQNI_CONNECTION + " BIGINT, "
      + COLUMN_SQNI_INPUT + " BIGINT, "
      + COLUMN_SQNI_VALUE + " LONG VARCHAR,"
      + "PRIMARY KEY (" + COLUMN_SQNI_CONNECTION + ", " + COLUMN_SQNI_INPUT + "), "
      + "CONSTRAINT " + CONSTRAINT_SQNI_SQN + " "
        + "FOREIGN KEY (" + COLUMN_SQNI_CONNECTION + ") "
          + "REFERENCES " + TABLE_SQ_CONNECTION + " (" + COLUMN_SQN_ID + "),"
      + "CONSTRAINT " + CONSTRAINT_SQNI_SQI + " "
        + "FOREIGN KEY (" + COLUMN_SQNI_INPUT + ") "
          + "REFERENCES " + TABLE_SQ_INPUT + " (" + COLUMN_SQI_ID + ")"
      + ")";

// DDL: Create table SQ_JOB_INPUT
 public static final String QUERY_CREATE_TABLE_SQ_JOB_INPUT =
     "CREATE TABLE " + TABLE_SQ_JOB_INPUT + " ("
     + COLUMN_SQBI_JOB + " BIGINT, "
     + COLUMN_SQBI_INPUT + " BIGINT, "
     + COLUMN_SQBI_VALUE + " LONG VARCHAR,"
     + " PRIMARY KEY (" + COLUMN_SQBI_JOB + ", " + COLUMN_SQBI_INPUT + "), "
     + " CONSTRAINT " + CONSTRAINT_SQBI_SQB + " "
       + "FOREIGN KEY (" + COLUMN_SQBI_JOB + ") "
       +  "REFERENCES " + TABLE_SQ_JOB + " (" + COLUMN_SQB_ID + "), "
     + " CONSTRAINT " + CONSTRAINT_SQBI_SQI + " "
       + "FOREIGN KEY (" + COLUMN_SQBI_INPUT + ") "
         + "REFERENCES " + TABLE_SQ_INPUT + " (" + COLUMN_SQI_ID + ")"
     + ")";

 // DDL: Create table SQ_SUBMISSION
 public static final String QUERY_CREATE_TABLE_SQ_SUBMISSION =
   "CREATE TABLE " + TABLE_SQ_SUBMISSION + " ("
   + COLUMN_SQS_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
   + COLUMN_SQS_JOB + " BIGINT, "
   + COLUMN_SQS_STATUS + " VARCHAR(20), "
   + COLUMN_SQS_CREATION_DATE + " TIMESTAMP,"
   + COLUMN_SQS_UPDATE_DATE + " TIMESTAMP,"
   + COLUMN_SQS_EXTERNAL_ID + " VARCHAR(50), "
   + COLUMN_SQS_EXTERNAL_LINK + " VARCHAR(150), "
   + COLUMN_SQS_EXCEPTION + " VARCHAR(150), "
   + COLUMN_SQS_EXCEPTION_TRACE + " VARCHAR(750), "
   + "PRIMARY KEY (" + COLUMN_SQS_ID + "), "
   + "CONSTRAINT " + CONSTRAINT_SQS_SQB + " "
     + "FOREIGN KEY (" + COLUMN_SQS_JOB + ") "
       + "REFERENCES " + TABLE_SQ_JOB + "("  + COLUMN_SQB_ID + ") ON DELETE CASCADE"
   +  ")";

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

 // DDL: Create table SQ_COUNTER_GROUP
 public static final String QUERY_CREATE_TABLE_SQ_COUNTER_GROUP =
   "CREATE TABLE " + TABLE_SQ_COUNTER_GROUP + " ("
   + COLUMN_SQG_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
   + COLUMN_SQG_NAME + " VARCHAR(75), "
   + "PRIMARY KEY (" + COLUMN_SQG_ID + "),"
   + "UNIQUE ( " + COLUMN_SQG_NAME + ")"
   + ")";

 // DDL: Create table SQ_COUNTER
 public static final String QUERY_CREATE_TABLE_SQ_COUNTER =
   "CREATE TABLE " + TABLE_SQ_COUNTER + " ("
   + COLUMN_SQR_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
   + COLUMN_SQR_NAME + " VARCHAR(75), "
   + "PRIMARY KEY (" + COLUMN_SQR_ID + "), "
   + "UNIQUE ( " + COLUMN_SQR_NAME + ")"
   + ")";

 // DDL: Create table SQ_COUNTER_SUBMISSION
 public static final String QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION =
   "CREATE TABLE " + TABLE_SQ_COUNTER_SUBMISSION + " ("
   + COLUMN_SQRS_GROUP + " BIGINT, "
   + COLUMN_SQRS_COUNTER + " BIGINT, "
   + COLUMN_SQRS_SUBMISSION + " BIGINT, "
   + COLUMN_SQRS_VALUE + " BIGINT, "
   + "PRIMARY KEY (" + COLUMN_SQRS_GROUP + ", " + COLUMN_SQRS_COUNTER + ", " + COLUMN_SQRS_SUBMISSION + "), "
   + "CONSTRAINT " + CONSTRAINT_SQRS_SQG + " "
     + "FOREIGN KEY (" + COLUMN_SQRS_GROUP + ") "
       + "REFERENCES " + TABLE_SQ_COUNTER_GROUP + "(" + COLUMN_SQG_ID + "), "
   + "CONSTRAINT " + CONSTRAINT_SQRS_SQR + " "
     + "FOREIGN KEY (" + COLUMN_SQRS_COUNTER + ") "
       + "REFERENCES " + TABLE_SQ_COUNTER + "(" + COLUMN_SQR_ID + "), "
   + "CONSTRAINT " + CONSTRAINT_SQRS_SQS + " "
     + "FOREIGN KEY (" + COLUMN_SQRS_SUBMISSION + ") "
       + "REFERENCES " + TABLE_SQ_SUBMISSION + "(" + COLUMN_SQS_ID + ") ON DELETE CASCADE "
   + ")";

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

 public static final String STMT_SELECT_SQD_ID_BY_SQD_NAME =
     "SELECT " + COLUMN_SQD_ID + " FROM " + TABLE_SQ_DIRECTION
         + " WHERE " + COLUMN_SQD_NAME + "=?";

 public static final String STMT_SELECT_SQD_NAME_BY_SQD_ID =
     "SELECT " + COLUMN_SQD_NAME + " FROM " + TABLE_SQ_DIRECTION
         + " WHERE " + COLUMN_SQD_ID + "=?";

  //DML: Get configurable by given name
  public static final String STMT_SELECT_FROM_CONFIGURABLE =
    "SELECT "
    + COLUMN_SQC_ID + ", "
    + COLUMN_SQC_NAME + ", "
    + COLUMN_SQC_CLASS + ", "
    + COLUMN_SQC_VERSION
    + " FROM " + TABLE_SQ_CONFIGURABLE
    + " WHERE " + COLUMN_SQC_NAME + " = ?";

  //DML: Get all configurables for a given type
  public static final String STMT_SELECT_CONFIGURABLE_ALL_FOR_TYPE =
  "SELECT "
  + COLUMN_SQC_ID + ", "
  + COLUMN_SQC_NAME + ", "
  + COLUMN_SQC_CLASS + ", "
  + COLUMN_SQC_VERSION
  + " FROM " + TABLE_SQ_CONFIGURABLE
  + " WHERE " + COLUMN_SQC_TYPE + " = ?";

  // DML: Select all connectors
  @Deprecated // used only for upgrade logic
  public static final String STMT_SELECT_CONNECTOR_ALL =
    "SELECT "
    + COLUMN_SQC_ID + ", "
    + COLUMN_SQC_NAME + ", "
    + COLUMN_SQC_CLASS + ", "
    + COLUMN_SQC_VERSION
    + " FROM " + TABLE_SQ_CONNECTOR;

  //DML: Get all configs for a given configurable
  public static final String STMT_SELECT_CONFIG_FOR_CONFIGURABLE =
     "SELECT "
     + COLUMN_SQ_CFG_ID + ", "
     + COLUMN_SQ_CFG_CONFIGURABLE + ", "
     + COLUMN_SQ_CFG_NAME + ", "
     + COLUMN_SQ_CFG_TYPE + ", "
     + COLUMN_SQ_CFG_INDEX
     + " FROM " + TABLE_SQ_CONFIG
     + " WHERE " + COLUMN_SQ_CFG_CONFIGURABLE + " = ? "
     + " ORDER BY " + COLUMN_SQ_CFG_INDEX;

   // DML: Get inputs for a given config
  public static final String STMT_SELECT_INPUT =
     "SELECT "
     + COLUMN_SQI_ID + ", "
     + COLUMN_SQI_NAME + ", "
     + COLUMN_SQI_CONFIG + ", "
     + COLUMN_SQI_INDEX + ", "
     + COLUMN_SQI_TYPE + ", "
     + COLUMN_SQI_STRMASK + ", "
     + COLUMN_SQI_STRLENGTH + ", "
     + COLUMN_SQI_ENUMVALS + ", "
     + "cast(null as varchar(100))"
     + " FROM " + TABLE_SQ_INPUT
     + " WHERE " + COLUMN_SQI_CONFIG + " = ?"
     + " ORDER BY " + COLUMN_SQI_INDEX;

  //DML: Get inputs and values for a given link
  public static final String STMT_FETCH_LINK_INPUT =
     "SELECT "
     + COLUMN_SQI_ID + ", "
     + COLUMN_SQI_NAME + ", "
     + COLUMN_SQI_CONFIG + ", "
     + COLUMN_SQI_INDEX + ", "
     + COLUMN_SQI_TYPE + ", "
     + COLUMN_SQI_STRMASK + ", "
     + COLUMN_SQI_STRLENGTH + ","
     + COLUMN_SQI_ENUMVALS + ", "
     + COLUMN_SQ_LNKI_VALUE
     + " FROM " + TABLE_SQ_INPUT
     + " LEFT OUTER JOIN " + TABLE_SQ_LINK_INPUT
       + " ON " + COLUMN_SQ_LNKI_INPUT + " = " + COLUMN_SQI_ID
       + " AND " + COLUMN_SQ_LNKI_LINK + " = ?"
     + " WHERE " + COLUMN_SQI_CONFIG + " = ?"
       + " AND (" + COLUMN_SQ_LNKI_LINK + " = ?" + " OR " + COLUMN_SQ_LNKI_LINK + " IS NULL)"
     + " ORDER BY " + COLUMN_SQI_INDEX;

  //DML: Fetch inputs and values for a given job
  public static final String STMT_FETCH_JOB_INPUT =
     "SELECT "
     + COLUMN_SQI_ID + ", "
     + COLUMN_SQI_NAME + ", "
     + COLUMN_SQI_CONFIG + ", "
     + COLUMN_SQI_INDEX + ", "
     + COLUMN_SQI_TYPE + ", "
     + COLUMN_SQI_STRMASK + ", "
     + COLUMN_SQI_STRLENGTH + ", "
     + COLUMN_SQI_ENUMVALS + ", "
     + COLUMN_SQBI_VALUE
     + " FROM " + TABLE_SQ_INPUT
     + " LEFT OUTER JOIN " + TABLE_SQ_JOB_INPUT
     + " ON " + COLUMN_SQBI_INPUT + " = " + COLUMN_SQI_ID
     + " AND  " + COLUMN_SQBI_JOB + " = ?"
     + " WHERE " + COLUMN_SQI_CONFIG + " = ?"
     + " AND (" + COLUMN_SQBI_JOB + " = ? OR " + COLUMN_SQBI_JOB + " IS NULL)"
     + " ORDER BY " + COLUMN_SQI_INDEX;

  //DML: Insert into configurable
  public static final String STMT_INSERT_INTO_CONFIGURABLE =
     "INSERT INTO " + TABLE_SQ_CONFIGURABLE + " ("
     + COLUMN_SQC_NAME + ", "
     + COLUMN_SQC_CLASS + ", "
     + COLUMN_SQC_VERSION + ", "
     + COLUMN_SQC_TYPE
     + ") VALUES (?, ?, ?, ?)";

  @Deprecated // used only in the upgrade path
  public static final String STMT_INSERT_INTO_CONFIGURABLE_WITHOUT_SUPPORTED_DIRECTIONS =
     "INSERT INTO " + TABLE_SQ_CONNECTOR+ " ("
         + COLUMN_SQC_NAME + ", "
         + COLUMN_SQC_CLASS + ", "
         + COLUMN_SQC_VERSION
         + ") VALUES (?, ?, ?)";

  //DML: Insert into config
  public static final String STMT_INSERT_INTO_CONFIG =
     "INSERT INTO " + TABLE_SQ_CONFIG + " ("
     + COLUMN_SQ_CFG_CONFIGURABLE + ", "
     + COLUMN_SQ_CFG_NAME + ", "
     + COLUMN_SQ_CFG_TYPE + ", "
     + COLUMN_SQ_CFG_INDEX
     + ") VALUES ( ?, ?, ?, ?)";

  //DML: Insert into config input
  public static final String STMT_INSERT_INTO_INPUT =
     "INSERT INTO " + TABLE_SQ_INPUT + " ("
     + COLUMN_SQI_NAME + ", "
     + COLUMN_SQI_CONFIG + ", "
     + COLUMN_SQI_INDEX + ", "
     + COLUMN_SQI_TYPE + ", "
     + COLUMN_SQI_STRMASK + ", "
     + COLUMN_SQI_STRLENGTH + ", "
     + COLUMN_SQI_ENUMVALS
     + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  //Delete all configs for a given configurable
  public static final String STMT_DELETE_CONFIGS_FOR_CONFIGURABLE =
   "DELETE FROM " + TABLE_SQ_CONFIG
   + " WHERE " + COLUMN_SQ_CFG_CONFIGURABLE + " = ?";

  //Delete all inputs for a given configurable
  public static final String STMT_DELETE_INPUTS_FOR_CONFIGURABLE =
   "DELETE FROM " + TABLE_SQ_INPUT
   + " WHERE "
   + COLUMN_SQI_CONFIG
   + " IN (SELECT "
   + COLUMN_SQ_CFG_ID
   + " FROM " + TABLE_SQ_CONFIG
   + " WHERE "
   + COLUMN_SQ_CFG_CONFIGURABLE + " = ?)";

  //Update the configurable
  public static final String STMT_UPDATE_CONFIGURABLE =
   "UPDATE " + TABLE_SQ_CONFIGURABLE
   + " SET " + COLUMN_SQC_NAME + " = ?, "
   + COLUMN_SQC_CLASS + " = ?, "
   + COLUMN_SQC_VERSION + " = ?, "
   + COLUMN_SQC_TYPE + " = ? "
   + " WHERE " + COLUMN_SQC_ID + " = ?";

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

  // DML: Insert new link
  public static final String STMT_INSERT_LINK =
    "INSERT INTO " + TABLE_SQ_LINK + " ("
    + COLUMN_SQ_LNK_NAME + ", "
    + COLUMN_SQ_LNK_CONFIGURABLE + ", "
    + COLUMN_SQ_LNK_ENABLED + ", "
    + COLUMN_SQ_LNK_CREATION_USER + ", "
    + COLUMN_SQ_LNK_CREATION_DATE + ", "
    + COLUMN_SQ_LNK_UPDATE_USER + ", "
    + COLUMN_SQ_LNK_UPDATE_DATE
    + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  // DML: Insert new link inputs
  public static final String STMT_INSERT_LINK_INPUT =
    "INSERT INTO " + TABLE_SQ_LINK_INPUT + " ("
    + COLUMN_SQ_LNKI_LINK + ", "
    + COLUMN_SQ_LNKI_INPUT + ", "
    + COLUMN_SQ_LNKI_VALUE
    + ") VALUES (?, ?, ?)";

  // DML: Update link
  public static final String STMT_UPDATE_LINK =
    "UPDATE " + TABLE_SQ_LINK + " SET "
    + COLUMN_SQ_LNK_NAME + " = ?, "
    + COLUMN_SQ_LNK_UPDATE_USER + " = ?, "
    + COLUMN_SQ_LNK_UPDATE_DATE + " = ? "
    + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Enable or disable link
  public static final String STMT_ENABLE_LINK =
    "UPDATE " + TABLE_SQ_LINK + " SET "
    + COLUMN_SQ_LNK_ENABLED + " = ? "
    + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Delete rows from link input table
  public static final String STMT_DELETE_LINK_INPUT =
    "DELETE FROM " + TABLE_SQ_LINK_INPUT
    + " WHERE " + COLUMN_SQ_LNKI_LINK + " = ?";

  // DML: Delete row from link table
  public static final String STMT_DELETE_LINK =
    "DELETE FROM " + TABLE_SQ_LINK
    + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Select one specific link
  public static final String STMT_SELECT_LINK_SINGLE =
    "SELECT "
    + COLUMN_SQ_LNK_ID + ", "
    + COLUMN_SQ_LNK_NAME + ", "
    + COLUMN_SQ_LNK_CONFIGURABLE + ", "
    + COLUMN_SQ_LNK_ENABLED + ", "
    + COLUMN_SQ_LNK_CREATION_USER + ", "
    + COLUMN_SQ_LNK_CREATION_DATE + ", "
    + COLUMN_SQ_LNK_UPDATE_USER + ", "
    + COLUMN_SQ_LNK_UPDATE_DATE
    + " FROM " + TABLE_SQ_LINK
    + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Select all links
  public static final String STMT_SELECT_LINK_ALL =
    "SELECT "
    + COLUMN_SQ_LNK_ID + ", "
    + COLUMN_SQ_LNK_NAME + ", "
    + COLUMN_SQ_LNK_CONFIGURABLE + ", "
    + COLUMN_SQ_LNK_ENABLED + ", "
    + COLUMN_SQ_LNK_CREATION_USER + ", "
    + COLUMN_SQ_LNK_CREATION_DATE + ", "
    + COLUMN_SQ_LNK_UPDATE_USER + ", "
    + COLUMN_SQ_LNK_UPDATE_DATE
    + " FROM " + TABLE_SQ_LINK;

  // DML: Select all links for a specific connector.
  public static final String STMT_SELECT_LINK_FOR_CONNECTOR_CONFIGURABLE =
    "SELECT "
    + COLUMN_SQ_LNK_ID + ", "
    + COLUMN_SQ_LNK_NAME + ", "
    + COLUMN_SQ_LNK_CONFIGURABLE + ", "
    + COLUMN_SQ_LNK_ENABLED + ", "
    + COLUMN_SQ_LNK_CREATION_USER + ", "
    + COLUMN_SQ_LNK_CREATION_DATE + ", "
    + COLUMN_SQ_LNK_UPDATE_USER + ", "
    + COLUMN_SQ_LNK_UPDATE_DATE
    + " FROM " + TABLE_SQ_LINK
    + " WHERE " + COLUMN_SQ_LNK_CONFIGURABLE + " = ?";

  // DML: Check if given link exists
  public static final String STMT_SELECT_LINK_CHECK_BY_ID =
    "SELECT count(*) FROM " + TABLE_SQ_LINK
    + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Insert new job
  public static final String STMT_INSERT_JOB =
    "INSERT INTO " + TABLE_SQ_JOB + " ("
    + COLUMN_SQB_NAME + ", "
    + COLUMN_SQB_FROM_LINK + ", "
    + COLUMN_SQB_TO_LINK + ", "
    + COLUMN_SQB_ENABLED + ", "
    + COLUMN_SQB_CREATION_USER + ", "
    + COLUMN_SQB_CREATION_DATE + ", "
    + COLUMN_SQB_UPDATE_USER + ", "
    + COLUMN_SQB_UPDATE_DATE
    + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

  // DML: Insert new job inputs
  public static final String STMT_INSERT_JOB_INPUT =
    "INSERT INTO " + TABLE_SQ_JOB_INPUT + " ("
    + COLUMN_SQBI_JOB + ", "
    + COLUMN_SQBI_INPUT + ", "
    + COLUMN_SQBI_VALUE
    + ") VALUES (?, ?, ?)";

  public static final String STMT_UPDATE_JOB =
    "UPDATE " + TABLE_SQ_JOB + " SET "
    + COLUMN_SQB_NAME + " = ?, "
    + COLUMN_SQB_UPDATE_USER + " = ?, "
    + COLUMN_SQB_UPDATE_DATE + " = ? "
    + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Enable or disable job
  public static final String STMT_ENABLE_JOB =
    "UPDATE " + TABLE_SQ_JOB + " SET "
    + COLUMN_SQB_ENABLED + " = ? "
    + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Delete rows from job input table
  public static final String STMT_DELETE_JOB_INPUT =
    "DELETE FROM " + TABLE_SQ_JOB_INPUT
    + " WHERE " + COLUMN_SQBI_JOB + " = ?";

  // DML: Delete row from job table
  public static final String STMT_DELETE_JOB =
    "DELETE FROM " + TABLE_SQ_JOB
    + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Check if given job exists
  public static final String STMT_SELECT_JOB_CHECK_BY_ID =
    "SELECT count(*) FROM " + TABLE_SQ_JOB
    + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Check if there are jobs for given link
  public static final String STMT_SELECT_JOBS_FOR_LINK_CHECK =
    "SELECT"
    + " count(*)"
    + " FROM " + TABLE_SQ_JOB
    + " JOIN " + TABLE_SQ_LINK
      + " ON " + COLUMN_SQB_FROM_LINK + " = " + COLUMN_SQ_LNK_ID
    + " WHERE " + COLUMN_SQ_LNK_ID + " = ? ";

 //DML: Select all jobs
 public static final String STMT_SELECT_JOB =
   "SELECT "
   + "FROM_CONNECTOR." + COLUMN_SQ_LNK_CONFIGURABLE + ", "
   + "TO_CONNECTOR." + COLUMN_SQ_LNK_CONFIGURABLE + ", "
   + "JOB." + COLUMN_SQB_ID + ", "
   + "JOB." + COLUMN_SQB_NAME + ", "
   + "JOB." + COLUMN_SQB_FROM_LINK + ", "
   + "JOB." + COLUMN_SQB_TO_LINK + ", "
   + "JOB." + COLUMN_SQB_ENABLED + ", "
   + "JOB." + COLUMN_SQB_CREATION_USER + ", "
   + "JOB." + COLUMN_SQB_CREATION_DATE + ", "
   + "JOB." + COLUMN_SQB_UPDATE_USER + ", "
   + "JOB." + COLUMN_SQB_UPDATE_DATE
   + " FROM " + TABLE_SQ_JOB + " JOB"
     + " LEFT JOIN " + TABLE_SQ_LINK + " FROM_CONNECTOR"
       + " ON " + COLUMN_SQB_FROM_LINK + " = FROM_CONNECTOR." + COLUMN_SQ_LNK_ID
     + " LEFT JOIN " + TABLE_SQ_LINK + " TO_CONNECTOR"
       + " ON " + COLUMN_SQB_TO_LINK + " = TO_CONNECTOR." + COLUMN_SQ_LNK_ID;

  // DML: Select one specific job
  public static final String STMT_SELECT_JOB_SINGLE_BY_ID =
      STMT_SELECT_JOB + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Select all jobs for a Connector
  public static final String STMT_SELECT_ALL_JOBS_FOR_CONNECTOR_CONFIGURABLE =
      STMT_SELECT_JOB
      + " WHERE FROM_LINK." + COLUMN_SQ_LNK_CONFIGURABLE + " = ? OR TO_LINK."
      + COLUMN_SQ_LNK_CONFIGURABLE + " = ?";

  // DML: Insert new submission
  public static final String STMT_INSERT_SUBMISSION =
    "INSERT INTO " + TABLE_SQ_SUBMISSION + "("
    + COLUMN_SQS_JOB + ", "
    + COLUMN_SQS_STATUS + ", "
    + COLUMN_SQS_CREATION_USER + ", "
    + COLUMN_SQS_CREATION_DATE + ", "
    + COLUMN_SQS_UPDATE_USER + ", "
    + COLUMN_SQS_UPDATE_DATE + ", "
    + COLUMN_SQS_EXTERNAL_ID + ", "
    + COLUMN_SQS_EXTERNAL_LINK + ", "
    + COLUMN_SQS_EXCEPTION + ", "
    + COLUMN_SQS_EXCEPTION_TRACE + ") "
    + " VALUES(?, ?, ?, ?, ?, ?, ?, substr(?, 1, 150) , substr(?, 1, 150), substr(?, 1, 750))";

  // DML: Update existing submission
  public static final String STMT_UPDATE_SUBMISSION =
    "UPDATE " + TABLE_SQ_SUBMISSION + " SET "
    + COLUMN_SQS_STATUS + " = ?, "
    + COLUMN_SQS_UPDATE_USER + " = ?, "
    + COLUMN_SQS_UPDATE_DATE + " = ?, "
    + COLUMN_SQS_EXCEPTION + " = ?, "
    + COLUMN_SQS_EXCEPTION_TRACE + " = ?"
    + " WHERE " + COLUMN_SQS_ID + " = ?";

  // DML: Check if given submission exists
  public static final String STMT_SELECT_SUBMISSION_CHECK =
    "SELECT"
    + " count(*)"
    + " FROM " + TABLE_SQ_SUBMISSION
    + " WHERE " + COLUMN_SQS_ID + " = ?";

  // DML: Purge old entries
  public static final String STMT_PURGE_SUBMISSIONS =
    "DELETE FROM " + TABLE_SQ_SUBMISSION
    + " WHERE " + COLUMN_SQS_UPDATE_DATE + " < ?";

  // DML: Get unfinished
  public static final String STMT_SELECT_SUBMISSION_UNFINISHED =
    "SELECT "
    + COLUMN_SQS_ID + ", "
    + COLUMN_SQS_JOB + ", "
    + COLUMN_SQS_STATUS + ", "
    + COLUMN_SQS_CREATION_USER + ", "
    + COLUMN_SQS_CREATION_DATE + ", "
    + COLUMN_SQS_UPDATE_USER + ", "
    + COLUMN_SQS_UPDATE_DATE + ", "
    + COLUMN_SQS_EXTERNAL_ID + ", "
    + COLUMN_SQS_EXTERNAL_LINK + ", "
    + COLUMN_SQS_EXCEPTION + ", "
    + COLUMN_SQS_EXCEPTION_TRACE
    + " FROM " + TABLE_SQ_SUBMISSION
    + " WHERE " + COLUMN_SQS_STATUS + " = ?";

  // DML : Get all submissions
  public static final String STMT_SELECT_SUBMISSIONS =
    "SELECT "
    + COLUMN_SQS_ID + ", "
    + COLUMN_SQS_JOB + ", "
    + COLUMN_SQS_STATUS + ", "
    + COLUMN_SQS_CREATION_USER + ", "
    + COLUMN_SQS_CREATION_DATE + ", "
    + COLUMN_SQS_UPDATE_USER + ", "
    + COLUMN_SQS_UPDATE_DATE + ", "
    + COLUMN_SQS_EXTERNAL_ID + ", "
    + COLUMN_SQS_EXTERNAL_LINK + ", "
    + COLUMN_SQS_EXCEPTION + ", "
    + COLUMN_SQS_EXCEPTION_TRACE
    + " FROM " + TABLE_SQ_SUBMISSION
    + " ORDER BY " + COLUMN_SQS_UPDATE_DATE + " DESC";

  // DML: Get submissions for a job
  public static final String STMT_SELECT_SUBMISSIONS_FOR_JOB =
    "SELECT "
    + COLUMN_SQS_ID + ", "
    + COLUMN_SQS_JOB + ", "
    + COLUMN_SQS_STATUS + ", "
    + COLUMN_SQS_CREATION_USER + ", "
    + COLUMN_SQS_CREATION_DATE + ", "
    + COLUMN_SQS_UPDATE_USER + ", "
    + COLUMN_SQS_UPDATE_DATE + ", "
    + COLUMN_SQS_EXTERNAL_ID + ", "
    + COLUMN_SQS_EXTERNAL_LINK + ", "
    + COLUMN_SQS_EXCEPTION + ", "
    + COLUMN_SQS_EXCEPTION_TRACE
    + " FROM " + TABLE_SQ_SUBMISSION
    + " WHERE " + COLUMN_SQS_JOB + " = ?"
    + " ORDER BY " + COLUMN_SQS_UPDATE_DATE + " DESC";

  // DML: Select counter group
  public static final String STMT_SELECT_COUNTER_GROUP =
    "SELECT "
    + COLUMN_SQG_ID + ", "
    + COLUMN_SQG_NAME + " "
    + "FROM " + TABLE_SQ_COUNTER_GROUP + " "
    + "WHERE " + COLUMN_SQG_NAME + " = substr(?, 1, 75)";

  // DML: Insert new counter group
  public static final String STMT_INSERT_COUNTER_GROUP =
    "INSERT INTO " + TABLE_SQ_COUNTER_GROUP + " ("
    + COLUMN_SQG_NAME + ") "
    + "VALUES (substr(?, 1, 75))";

  // DML: Select counter
  public static final String STMT_SELECT_COUNTER =
    "SELECT "
    + COLUMN_SQR_ID + ", "
    + COLUMN_SQR_NAME + " "
    + "FROM " + TABLE_SQ_COUNTER + " "
    + "WHERE " + COLUMN_SQR_NAME + " = substr(?, 1, 75)";

  // DML: Insert new counter
  public static final String STMT_INSERT_COUNTER =
    "INSERT INTO " + TABLE_SQ_COUNTER + " ("
    + COLUMN_SQR_NAME + ") "
    + "VALUES (substr(?, 1, 75))";

  // DML: Insert new counter submission
  public static final String STMT_INSERT_COUNTER_SUBMISSION =
    "INSERT INTO " + TABLE_SQ_COUNTER_SUBMISSION + " ("
    + COLUMN_SQRS_GROUP + ", "
    + COLUMN_SQRS_COUNTER + ", "
    + COLUMN_SQRS_SUBMISSION + ", "
    + COLUMN_SQRS_VALUE + ") "
    + "VALUES (?, ?, ?, ?)";

  // DML: Select counter submission
  public static final String STMT_SELECT_COUNTER_SUBMISSION =
    "SELECT "
    + COLUMN_SQG_NAME + ", "
    + COLUMN_SQR_NAME + ", "
    + COLUMN_SQRS_VALUE + " "
    + "FROM " + TABLE_SQ_COUNTER_SUBMISSION + " "
    + "LEFT JOIN " + TABLE_SQ_COUNTER_GROUP
      + " ON " + COLUMN_SQRS_GROUP + " = " + COLUMN_SQG_ID + " "
    + "LEFT JOIN " + TABLE_SQ_COUNTER
      + " ON " + COLUMN_SQRS_COUNTER + " = " + COLUMN_SQR_ID + " "
    + "WHERE " + COLUMN_SQRS_SUBMISSION + " = ? ";

  // DML: Delete rows from counter submission table
  public static final String STMT_DELETE_COUNTER_SUBMISSION =
    "DELETE FROM " + TABLE_SQ_COUNTER_SUBMISSION
    + " WHERE " + COLUMN_SQRS_SUBMISSION + " = ?";

  // DDL: Increased size of SQ_CONNECTOR.SQC_VERSION to 64
  public static final String QUERY_UPGRADE_TABLE_SQ_CONNECTOR_MODIFY_COLUMN_SQC_VERSION_VARCHAR_64 =
    "ALTER TABLE " + TABLE_SQ_CONNECTOR + " ALTER COLUMN "
      + COLUMN_SQC_VERSION + " SET DATA TYPE VARCHAR(64)";

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

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_OPERATION_TO_SQF_DIRECTION =
      "UPDATE " + TABLE_SQ_FORM + " SET " + COLUMN_SQF_DIRECTION
        + "=? WHERE " + COLUMN_SQF_DIRECTION + "=?"
          + " AND " + COLUMN_SQF_CONNECTOR + " IS NOT NULL";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR =
      "UPDATE " + TABLE_SQ_FORM + " SET " + COLUMN_SQF_CONNECTOR + "= ?"
          + " WHERE " + COLUMN_SQF_CONNECTOR + " IS NULL AND "
          + COLUMN_SQF_NAME + " IN (?, ?)";

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

  /**
   * Intended to rename forms based on direction.
   * e.g. If SQ_FORM.SQF_NAME = 'table' and parameter 1 = 'from'
   * then SQ_FORM.SQF_NAME = 'fromTable'.
   */
  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_INPUT_NAMES =
      "UPDATE " + TABLE_SQ_INPUT + " SET "
          + COLUMN_SQI_NAME + "=("
          + "? || UPPER(SUBSTR(" + COLUMN_SQI_NAME + ",1,1))"
          + " || SUBSTR(" + COLUMN_SQI_NAME + ",2) )"
          + " WHERE " + COLUMN_SQI_FORM + " IN ("
          + " SELECT " + COLUMN_SQF_ID + " FROM " + TABLE_SQ_FORM + " WHERE " + COLUMN_SQF_NAME + "= ?"
          + " AND " + COLUMN_SQF_DIRECTION + "= ?)";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_DIRECTION_TO_NULL =
      "UPDATE " + TABLE_SQ_FORM + " SET "
        + COLUMN_SQF_DIRECTION + "= NULL"
        + " WHERE " + COLUMN_SQF_NAME + "= ?";

  public static final String QUERY_SELECT_THROTTLING_FORM_INPUT_IDS =
      "SELECT SQI." + COLUMN_SQI_ID + " FROM " + TABLE_SQ_INPUT + " SQI"
          + " INNER JOIN " + TABLE_SQ_FORM + " SQF ON SQI." + COLUMN_SQI_FORM + "=SQF." + COLUMN_SQF_ID
          + " WHERE SQF." + COLUMN_SQF_NAME + "='throttling' AND SQF." + COLUMN_SQF_DIRECTION + "=?";

  /**
   * Intended to change SQ_JOB_INPUT.SQBI_INPUT from EXPORT
   * throttling form, to IMPORT throttling form.
   */
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

 // Config and Connector directions
  public static final String STMT_INSERT_DIRECTION = "INSERT INTO " + TABLE_SQ_DIRECTION + " "
     + "(" + COLUMN_SQD_NAME + ") VALUES (?)";

  public static final String STMT_FETCH_CONFIG_DIRECTIONS =
     "SELECT "
         + COLUMN_SQ_CFG_ID + ", "
         + COLUMN_SQ_CFG_DIRECTION
         + " FROM " + TABLE_SQ_CONFIG;

  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIG_DROP_COLUMN_SQ_CFG_DIRECTION_VARCHAR =
     "ALTER TABLE " + TABLE_SQ_CONFIG + " DROP COLUMN " + COLUMN_SQ_CFG_DIRECTION;


  public static final String STMT_INSERT_SQ_CONNECTOR_DIRECTIONS =
     "INSERT INTO " + TABLE_SQ_CONNECTOR_DIRECTIONS + " "
         + "(" + COLUMN_SQCD_CONNECTOR + ", " + COLUMN_SQCD_DIRECTION + ")"
         + " VALUES (?, ?)";

  public static final String STMT_INSERT_SQ_CONFIG_DIRECTIONS =
     "INSERT INTO " + TABLE_SQ_CONFIG_DIRECTIONS + " "
         + "(" + COLUMN_SQ_CFG_DIR_CONFIG + ", " + COLUMN_SQ_CFG_DIR_DIRECTION + ")"
         + " VALUES (?, ?)";

  public static final String STMT_SELECT_SQ_CONNECTOR_DIRECTIONS_ALL =
     "SELECT " + COLUMN_SQCD_CONNECTOR + ", " + COLUMN_SQCD_DIRECTION
         + " FROM " + TABLE_SQ_CONNECTOR_DIRECTIONS;

  public static final String STMT_SELECT_SQ_CONNECTOR_DIRECTIONS =
     STMT_SELECT_SQ_CONNECTOR_DIRECTIONS_ALL + " WHERE "
         + COLUMN_SQCD_CONNECTOR + " = ?";

  public static final String STMT_SELECT_SQ_CONFIG_DIRECTIONS_ALL =
     "SELECT " + COLUMN_SQ_CFG_DIR_CONFIG + ", " + COLUMN_SQ_CFG_DIR_DIRECTION
         + " FROM " + TABLE_SQ_CONFIG_DIRECTIONS;

  public static final String STMT_SELECT_SQ_CONFIG_DIRECTIONS =
     STMT_SELECT_SQ_CONFIG_DIRECTIONS_ALL + " WHERE "
         + COLUMN_SQ_CFG_DIR_CONFIG + " = ?";

  //add unique constraint on the configurable table
  public static final String QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_UNIQUE_CONSTRAINT_NAME = "ALTER TABLE "
      + TABLE_SQ_CONFIGURABLE + " ADD CONSTRAINT " + CONSTRAINT_SQ_CONFIGURABLE_UNIQUE + " UNIQUE ("
      + COLUMN_SQC_NAME + ")";

  private DerbySchemaQuery() {
    // Disable explicit object creation
  }
}