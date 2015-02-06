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
 *    | SQCD_CONNECTOR: BIGINT       |FK SQ_CONFIGURABLE(SQC_ID)
 *    | SQCD_DIRECTION: BIGINT       |FK SQ_DIRECTION(SQD_ID)
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
 *    | SQ_CFG_CONNECTOR: BIGINT            |FK SQ_CONFIGURABLE(SQC_ID), NULL for driver
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
 *    | SQCD_CONFIG: BIGINT          |FK SQ_CONFIG(SQ_CFG_ID)
 *    | SQCD_DIRECTION: BIGINT       |FK SQ_DIRECTION(SQD_ID)
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
 *    | SQI_EDITABLE: VARCHAR(32)  |
 *    +----------------------------+
 * </pre>
* <p>
 * <strong>SQ_INPUT_RELATION</strong>: Input to Input relationship
 *
 * <pre>
 *    +----------------------------+
 *    | SQ_INPUT_RELATION           |
 *    +----------------------------+
 *    | SQIR_ID: BIGINT PK AUTO-GEN |
 *    | SQIR_PARENT_ID: BIGINT      |FK SQ_INPUT(SQI_ID)
 *    | SQIR_CHILD_ID: BIGINT       |FK SQ_INPUT(SQI_ID)
 *    +----------------------------+
 * </pre>
 *
 * </p>
 * <p>
 * <strong>SQ_LINK</strong>: Stored links
 *
 * <pre>
 *    +-----------------------------------+
 *    | SQ_LINK                           |
 *    +-----------------------------------+
 *    | SQ_LNK_ID: BIGINT PK AUTO-GEN     |
 *    | SQ_LNK_NAME: VARCHAR(64)          |
 *    | SQ_LNK_CONNECTOR: BIGINT          | FK SQ_CONFIGURABLE(SQC_ID)
 *    | SQ_LNK_CREATION_USER: VARCHAR(32) |
 *    | SQ_LNK_CREATION_DATE: TIMESTAMP   |
 *    | SQ_LNK_UPDATE_USER: VARCHAR(32)   |
 *    | SQ_LNK_UPDATE_DATE: TIMESTAMP     |
 *    | SQ_LNK_ENABLED: BOOLEAN           |
 *    +-----------------------------------+
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
   "CREATE SCHEMA " + CommonRepoUtils.escapeSchemaName(SCHEMA_SQOOP);

  public static final String QUERY_SYSSCHEMA_SQOOP =
   "SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '"
   + SCHEMA_SQOOP + "'";

  // DDL: Create table SQ_SYSTEM
  public static final String QUERY_CREATE_TABLE_SQ_SYSTEM =
    "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SYSTEM_NAME) + " ("
    + CommonRepoUtils.escapeColumnName(COLUMN_SQM_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQM_KEY) + " VARCHAR(64), "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQM_VALUE) + " VARCHAR(64) "
    + ")";

  // DDL: Create table SQ_DIRECTION
  public static final String QUERY_CREATE_TABLE_SQ_DIRECTION =
   "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_DIRECTION_NAME) + " ("
   + CommonRepoUtils.escapeColumnName(COLUMN_SQD_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQD_NAME) + " VARCHAR(64)"
   + ")";

  // DDL: Create table SQ_CONNECTOR
  @Deprecated // used only for upgrade
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + " VARCHAR(64), "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_CLASS) + " VARCHAR(255), "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION) + " VARCHAR(64) "
      + ")";

  // DDL: Create table SQ_CONNECTOR_DIRECTIONS
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_DIRECTIONS_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_CONNECTOR) + " BIGINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_DIRECTION) + " BIGINT, "
      + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQCD_SQC_NAME) + " "
        + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_CONNECTOR) + ") "
          + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + "), "
      + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQCD_SQD_NAME) + " "
        + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_DIRECTION) + ") "
          + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_DIRECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQD_ID) + ")"
      + ")";

   // DDL: Create table SQ_FORM
  public static final String QUERY_CREATE_TABLE_SQ_FORM =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + " BIGINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQF_OPERATION) + " VARCHAR(32), "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQF_NAME) + " VARCHAR(64), "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQF_TYPE) + " VARCHAR(32), "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQF_INDEX) + " SMALLINT, "
      + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQF_SQC_NAME) + " "
        + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQF_CONNECTOR) + ") "
          + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")"
      + ")";

  // DDL: Create table SQ_CONFIG_DIRECTIONS ( same as SQ_FORM_DIRECTIONS)
  // Note: that the form was renamed to config at one point and this code was added after the rename
  // DDL: Create table SQ_CONFIG_DIRECTIONS
 public static final String QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS =
     "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_DIRECTIONS_NAME) + " ("
     + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_CONFIG) + " BIGINT, "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_DIRECTION) + " BIGINT, "
     + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_CFG_DIR_CONFIG_NAME) + " "
       + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_CONFIG) + ") "
         + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID) + "), "
     + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQ_CFG_DIR_DIRECTION_NAME) + " "
       + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_DIRECTION) + ") "
         + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_DIRECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQD_ID) + ")"
     + ")";


  // DDL: Create table SQ_INPUT
  public static final String QUERY_CREATE_TABLE_SQ_INPUT =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + " VARCHAR(64), "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + " BIGINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX) + " SMALLINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + " VARCHAR(32), "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRMASK) + " BOOLEAN, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRLENGTH) + " SMALLINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ENUMVALS) + " VARCHAR(100),"
      + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQI_SQF_NAME) + " "
        + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_FORM) + ") "
          + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_FORM_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQF_ID) + ")"
      + ")";

    // DDL : Create table SQ_INPUT_RELATION
    public static final String QUERY_CREATE_TABLE_SQ_INPUT_RELATION =
        "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_RELATION_NAME) + " ("
        + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_PARENT) + " BIGINT, "
        + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_CHILD) + " BIGINT, "
        + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQIR_PARENT_NAME) + " "
          + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_PARENT) + ") "
            + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + "),"
        + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQIR_CHILD_NAME) + " "
          + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_CHILD) + ") "
            + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ")"
        + ")";

  // DDL: Create table SQ_CONNECTION
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTION =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CONNECTOR) + " BIGINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_NAME)  + " VARCHAR(32),"
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CREATION_DATE) + " TIMESTAMP,"
      + CommonRepoUtils.escapeColumnName(COLUMN_SQN_UPDATE_DATE) + " TIMESTAMP,"
      + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQN_SQC_NAME) + " "
        + "FOREIGN KEY(" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_CONNECTOR) + ") "
          + " REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ")"
      + ")";
//DDL: Create table SQ_JOB
 public static final String QUERY_CREATE_TABLE_SQ_JOB =
     "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ("
     + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CONNECTION) + " BIGINT, "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + " VARCHAR(64), "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TYPE) + " VARCHAR(64),"
     + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CREATION_DATE) + " TIMESTAMP,"
     + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_DATE) + " TIMESTAMP,"
     + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQB_SQN_NAME) + " "
       + "FOREIGN KEY(" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CONNECTION) + ") "
         + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + ")"
     + ")";


  // DDL: Create table SQ_CONNECTION_INPUT
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTION_INPUT =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_INPUT_NAME) + " ("
      + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_CONNECTION) + " BIGINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_INPUT) + " BIGINT, "
      + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_VALUE) + " LONG VARCHAR,"
      + "PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_CONNECTION) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_INPUT) + "), "
      + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQNI_SQN_NAME) + " "
        + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_CONNECTION) + ") "
          + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTION_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQN_ID) + "),"
      + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQNI_SQI_NAME) + " "
        + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQNI_INPUT) + ") "
          + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ")"
      + ")";

// DDL: Create table SQ_JOB_INPUT
 public static final String QUERY_CREATE_TABLE_SQ_JOB_INPUT =
     "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME) + " ("
     + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + " BIGINT, "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + " BIGINT, "
     + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_VALUE) + " LONG VARCHAR,"
     + " PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + "), "
     + " CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQBI_SQB_NAME) + " "
       + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + ") "
       +  "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + "), "
     + " CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQBI_SQI_NAME) + " "
       + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + ") "
         + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " (" + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ")"
     + ")";

 // DDL: Create table SQ_SUBMISSION
 public static final String QUERY_CREATE_TABLE_SQ_SUBMISSION =
   "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + " ("
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + " BIGINT, "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_STATUS) + " VARCHAR(20), "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_DATE) + " TIMESTAMP,"
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + " TIMESTAMP,"
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_ID) + " VARCHAR(50), "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_LINK) + " VARCHAR(150), "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXCEPTION) + " VARCHAR(150), "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXCEPTION_TRACE) + " VARCHAR(750), "
   + "PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + "), "
   + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQS_SQB_NAME) + " "
     + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + ") "
       + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + "("  + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + ") ON DELETE CASCADE"
   +  ")";

 // DDL: Create table SQ_COUNTER_GROUP
 public static final String QUERY_CREATE_TABLE_SQ_COUNTER_GROUP =
   "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_GROUP_NAME) + " ("
   + CommonRepoUtils.escapeColumnName(COLUMN_SQG_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQG_NAME) + " VARCHAR(75), "
   + "PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQG_ID) + "),"
   + "UNIQUE ( " + CommonRepoUtils.escapeColumnName(COLUMN_SQG_NAME) + ")"
   + ")";

 // DDL: Create table SQ_COUNTER
 public static final String QUERY_CREATE_TABLE_SQ_COUNTER =
   "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_NAME) + " ("
   + CommonRepoUtils.escapeColumnName(COLUMN_SQR_ID) + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQR_NAME) + " VARCHAR(75), "
   + "PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQR_ID) + "), "
   + "UNIQUE ( " + CommonRepoUtils.escapeColumnName(COLUMN_SQR_NAME) + ")"
   + ")";

 // DDL: Create table SQ_COUNTER_SUBMISSION
 public static final String QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION =
   "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_SUBMISSION_NAME) + " ("
   + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_GROUP) + " BIGINT, "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_COUNTER) + " BIGINT, "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_SUBMISSION) + " BIGINT, "
   + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_VALUE) + " BIGINT, "
   + "PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_GROUP) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_COUNTER) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_SUBMISSION) + "), "
   + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQRS_SQG_NAME) + " "
     + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_GROUP) + ") "
       + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_GROUP_NAME) + "(" + CommonRepoUtils.escapeColumnName(COLUMN_SQG_ID) + "), "
   + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQRS_SQR_NAME) + " "
     + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_COUNTER) + ") "
       + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_NAME) + "(" + CommonRepoUtils.escapeColumnName(COLUMN_SQR_ID) + "), "
   + "CONSTRAINT " + CommonRepoUtils.getConstraintName(SCHEMA_SQOOP, CONSTRAINT_SQRS_SQS_NAME) + " "
     + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_SUBMISSION) + ") "
       + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + "(" + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + ") ON DELETE CASCADE "
   + ")";

  private DerbySchemaCreateQuery() {

  }
}