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
public final class DerbySchemaCreateQuery {

  /**************************** DERBY CREATE SCHEMA queries ********************************/
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
  @Deprecated // used only for upgrade
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

  private DerbySchemaCreateQuery() {

  }
}