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
 * <pre>
 *    +----------------------------+
 *    | SQ_SYSTEM                  |
 *    +----------------------------+
 *    | SQM_ID: BIGINT PK          |
 *    | SQM_KEY: VARCHAR(64)       |
 *    | SQM_VALUE: VARCHAR(64)     |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONNECTOR</strong>: Connector registration.
 * <pre>
 *    +----------------------------+
 *    | SQ_CONNECTOR               |
 *    +----------------------------+
 *    | SQC_ID: BIGINT PK AUTO-GEN |
 *    | SQC_NAME: VARCHAR(64)      |
 *    | SQC_CLASS: VARCHAR(255)    |
 *    | SQC_VERSION: VARCHAR(64)   |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_FORM</strong>: Form details.
 * <pre>
 *    +----------------------------------+
 *    | SQ_FORM                          |
 *    +----------------------------------+
 *    | SQF_ID: BIGINT PK AUTO-GEN       |
 *    | SQF_CONNECTOR: BIGINT            | FK SQ_CONNECTOR(SQC_ID),NULL for framework
 *    | SQF_DIRECTION: VARCHAR(32)       | "FROM"|"TO"|NULL
 *    | SQF_NAME: VARCHAR(64)            |
 *    | SQF_TYPE: VARCHAR(32)            | "CONNECTION"|"JOB"
 *    | SQF_INDEX: SMALLINT              |
 *    +----------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_INPUT</strong>: Input details
 * <pre>
 *    +----------------------------+
 *    | SQ_INPUT                   |
 *    +----------------------------+
 *    | SQI_ID: BIGINT PK AUTO-GEN |
 *    | SQI_NAME: VARCHAR(64)      |
 *    | SQI_FORM: BIGINT           | FK SQ_FORM(SQF_ID)
 *    | SQI_INDEX: SMALLINT        |
 *    | SQI_TYPE: VARCHAR(32)      | "STRING"|"MAP"
 *    | SQI_STRMASK: BOOLEAN       |
 *    | SQI_STRLENGTH: SMALLINT    |
 *    | SQI_ENUMVALS: VARCHAR(100) |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONNECTION</strong>: Stored connections
 * <pre>
 *    +--------------------------------+
 *    | SQ_CONNECTION                  |
 *    +--------------------------------+
 *    | SQN_ID: BIGINT PK AUTO-GEN     |
 *    | SQN_NAME: VARCHAR(64)          |
 *    | SQN_CONNECTOR: BIGINT          | FK SQ_CONNECTOR(SQC_ID)
 *    | SQN_CREATION_USER: VARCHAR(32) |
 *    | SQN_CREATION_DATE: TIMESTAMP   |
 *    | SQN_UPDATE_USER: VARCHAR(32)   |
 *    | SQN_UPDATE_DATE: TIMESTAMP     |
 *    | SQN_ENABLED: BOOLEAN           |
 *    +--------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_JOB</strong>: Stored jobs
 * <pre>
 *    +--------------------------------+
 *    | SQ_JOB                         |
 *    +--------------------------------+
 *    | SQB_ID: BIGINT PK AUTO-GEN     |
 *    | SQB_NAME: VARCHAR(64)          |
 *    | SQB_FROM_CONNECTION: BIGINT    | FK SQ_CONNECTION(SQN_ID)
 *    | SQB_TO_CONNECTION: BIGINT      | FK SQ_CONNECTION(SQN_ID)
 *    | SQB_CREATION_USER: VARCHAR(32) |
 *    | SQB_CREATION_DATE: TIMESTAMP   |
 *    | SQB_UPDATE_USER: VARCHAR(32)   |
 *    | SQB_UPDATE_DATE: TIMESTAMP     |
 *    | SQB_ENABLED: BOOLEAN           |
 *    +--------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONNECTION_INPUT</strong>: N:M relationship connection and input
 * <pre>
 *    +----------------------------+
 *    | SQ_CONNECTION_INPUT        |
 *    +----------------------------+
 *    | SQNI_CONNECTION: BIGINT PK | FK SQ_CONNECTION(SQN_ID)
 *    | SQNI_INPUT: BIGINT PK      | FK SQ_INPUT(SQI_ID)
 *    | SQNI_VALUE: LONG VARCHAR   |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_JOB_INPUT</strong>: N:M relationship job and input
 * <pre>
 *    +----------------------------+
 *    | SQ_JOB_INPUT               |
 *    +----------------------------+
 *    | SQBI_JOB: BIGINT PK        | FK SQ_JOB(SQB_ID)
 *    | SQBI_INPUT: BIGINT PK      | FK SQ_INPUT(SQI_ID)
 *    | SQBI_VALUE: LONG VARCHAR   |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_SUBMISSION</strong>: List of submissions
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
 * </p>
 * <p>
 * <strong>SQ_COUNTER_GROUP</strong>: List of counter groups
 * <pre>
 *    +----------------------------+
 *    | SQ_COUNTER_GROUP           |
 *    +----------------------------+
 *    | SQG_ID: BIGINT PK          |
 *    | SQG_NAME: VARCHAR(75)      |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_COUNTER</strong>: List of counters
 * <pre>
 *    +----------------------------+
 *    | SQ_COUNTER                 |
 *    +----------------------------+
 *    | SQR_ID: BIGINT PK          |
 *    | SQR_NAME: VARCHAR(75)      |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_COUNTER_SUBMISSION</strong>: N:M Relationship
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
 * </p>
 */
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

  // DDL: Create table SQ_CONNECTOR
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR =
      "CREATE TABLE " + TABLE_SQ_CONNECTOR + " ("
      + COLUMN_SQC_ID + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
      + COLUMN_SQC_NAME + " VARCHAR(64), "
      + COLUMN_SQC_CLASS + " VARCHAR(255), "
      + COLUMN_SQC_VERSION + " VARCHAR(64) "
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

  // DDL: Add enabled column to table SQ_CONNECTION
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

  // DDL: Create table SQ_JOB
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

  // DML: Fetch connector Given Name
  public static final String STMT_FETCH_BASE_CONNECTOR =
      "SELECT "
      + COLUMN_SQC_ID + ", "
      + COLUMN_SQC_NAME + ", "
      + COLUMN_SQC_CLASS + ", "
      + COLUMN_SQC_VERSION
      + " FROM " + TABLE_SQ_CONNECTOR
      + " WHERE " + COLUMN_SQC_NAME + " = ?";

  // DML: Select all connectors
  public static final String STMT_SELECT_CONNECTOR_ALL =
    "SELECT "
    + COLUMN_SQC_ID + ", "
    + COLUMN_SQC_NAME + ", "
    + COLUMN_SQC_CLASS + ", "
    + COLUMN_SQC_VERSION
    + " FROM " + TABLE_SQ_CONNECTOR;

  // DML: Fetch all forms for a given connector
  public static final String STMT_FETCH_FORM_CONNECTOR =
      "SELECT "
      + COLUMN_SQF_ID + ", "
      + COLUMN_SQF_CONNECTOR + ", "
      + COLUMN_SQF_DIRECTION + ", "
      + COLUMN_SQF_NAME + ", "
      + COLUMN_SQF_TYPE + ", "
      + COLUMN_SQF_INDEX
      + " FROM " + TABLE_SQ_FORM
      + " WHERE " + COLUMN_SQF_CONNECTOR + " = ? "
      + " ORDER BY " + COLUMN_SQF_INDEX;

  // DML: Fetch all framework forms
  public static final String STMT_FETCH_FORM_FRAMEWORK =
      "SELECT "
      + COLUMN_SQF_ID + ", "
      + COLUMN_SQF_CONNECTOR + ", "
      + COLUMN_SQF_DIRECTION + ", "
      + COLUMN_SQF_NAME + ", "
      + COLUMN_SQF_TYPE + ", "
      + COLUMN_SQF_INDEX
      + " FROM " + TABLE_SQ_FORM
      + " WHERE " + COLUMN_SQF_CONNECTOR + " IS NULL "
      + " ORDER BY " + COLUMN_SQF_TYPE + ", " + COLUMN_SQF_DIRECTION  + ", " + COLUMN_SQF_INDEX;

  // DML: Fetch inputs for a given form
  public static final String STMT_FETCH_INPUT =
      "SELECT "
      + COLUMN_SQI_ID + ", "
      + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", "
      + COLUMN_SQI_INDEX + ", "
      + COLUMN_SQI_TYPE + ", "
      + COLUMN_SQI_STRMASK + ", "
      + COLUMN_SQI_STRLENGTH + ", "
      + COLUMN_SQI_ENUMVALS + ", "
      + "cast(null as varchar(100))"
      + " FROM " + TABLE_SQ_INPUT
      + " WHERE " + COLUMN_SQI_FORM + " = ?"
      + " ORDER BY " + COLUMN_SQI_INDEX;

  // DML: Fetch inputs and values for a given connection
  public static final String STMT_FETCH_CONNECTION_INPUT =
      "SELECT "
      + COLUMN_SQI_ID + ", "
      + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", "
      + COLUMN_SQI_INDEX + ", "
      + COLUMN_SQI_TYPE + ", "
      + COLUMN_SQI_STRMASK + ", "
      + COLUMN_SQI_STRLENGTH + ","
      + COLUMN_SQI_ENUMVALS + ", "
      + COLUMN_SQNI_VALUE
      + " FROM " + TABLE_SQ_INPUT
      + " LEFT OUTER JOIN " + TABLE_SQ_CONNECTION_INPUT
        + " ON " + COLUMN_SQNI_INPUT + " = " + COLUMN_SQI_ID
        + " AND " + COLUMN_SQNI_CONNECTION + " = ?"
      + " WHERE " + COLUMN_SQI_FORM + " = ?"
        + " AND (" + COLUMN_SQNI_CONNECTION + " = ?" + " OR " + COLUMN_SQNI_CONNECTION + " IS NULL)"
      + " ORDER BY " + COLUMN_SQI_INDEX;

  // DML: Fetch inputs and values for a given job
  public static final String STMT_FETCH_JOB_INPUT =
      "SELECT "
      + COLUMN_SQI_ID + ", "
      + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", "
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
      + " WHERE " + COLUMN_SQI_FORM + " = ?"
      + " AND (" + COLUMN_SQBI_JOB + " = ? OR " + COLUMN_SQBI_JOB + " IS NULL)"
      + " ORDER BY " + COLUMN_SQI_INDEX;

  // DML: Insert connector base
  public static final String STMT_INSERT_CONNECTOR_BASE =
      "INSERT INTO " + TABLE_SQ_CONNECTOR + " ("
      + COLUMN_SQC_NAME + ", "
      + COLUMN_SQC_CLASS + ", "
      + COLUMN_SQC_VERSION
      + ") VALUES (?, ?, ?)";

  // DML: Insert form base
  public static final String STMT_INSERT_FORM_BASE =
      "INSERT INTO " + TABLE_SQ_FORM + " ("
      + COLUMN_SQF_CONNECTOR + ", "
      + COLUMN_SQF_DIRECTION + ", "
      + COLUMN_SQF_NAME + ", "
      + COLUMN_SQF_TYPE + ", "
      + COLUMN_SQF_INDEX
      + ") VALUES ( ?, ?, ?, ?, ?)";

  // DML: Insert form input
  public static final String STMT_INSERT_INPUT_BASE =
      "INSERT INTO " + TABLE_SQ_INPUT + " ("
      + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", "
      + COLUMN_SQI_INDEX + ", "
      + COLUMN_SQI_TYPE + ", "
      + COLUMN_SQI_STRMASK + ", "
      + COLUMN_SQI_STRLENGTH + ", "
      + COLUMN_SQI_ENUMVALS
      + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  // Delete all forms for a given connector
  public static final String STMT_DELETE_FORMS_FOR_CONNECTOR =
    "DELETE FROM " + TABLE_SQ_FORM
    + " WHERE " + COLUMN_SQF_CONNECTOR + " = ?";

  // Delete all inputs for a given connector
  public static final String STMT_DELETE_INPUTS_FOR_CONNECTOR =
    "DELETE FROM " + TABLE_SQ_INPUT
    + " WHERE "
    + COLUMN_SQI_FORM
    + " IN (SELECT "
    + COLUMN_SQF_ID
    + " FROM " + TABLE_SQ_FORM
    + " WHERE "
    + COLUMN_SQF_CONNECTOR + " = ?)";

  // Delete all framework inputs
  public static final String STMT_DELETE_FRAMEWORK_INPUTS =
    "DELETE FROM " + TABLE_SQ_INPUT
    + " WHERE "
    + COLUMN_SQI_FORM
    + " IN (SELECT "
    + COLUMN_SQF_ID
    + " FROM " + TABLE_SQ_FORM
    + " WHERE "
    + COLUMN_SQF_CONNECTOR + " IS NULL)";

  // Delete all framework forms
  public static final String STMT_DELETE_FRAMEWORK_FORMS =
    "DELETE FROM " + TABLE_SQ_FORM
    + " WHERE " + COLUMN_SQF_CONNECTOR + " IS NULL";



  // Update the connector
  public static final String STMT_UPDATE_CONNECTOR =
    "UPDATE " + TABLE_SQ_CONNECTOR
    + " SET " + COLUMN_SQC_NAME + " = ?, "
    + COLUMN_SQC_CLASS + " = ?, "
    + COLUMN_SQC_VERSION + " = ? "
    + " WHERE " + COLUMN_SQC_ID + " = ?";

  // DML: Insert new connection
  public static final String STMT_INSERT_CONNECTION =
    "INSERT INTO " + TABLE_SQ_CONNECTION + " ("
    + COLUMN_SQN_NAME + ", "
    + COLUMN_SQN_CONNECTOR + ", "
    + COLUMN_SQN_ENABLED + ", "
    + COLUMN_SQN_CREATION_USER + ", "
    + COLUMN_SQN_CREATION_DATE + ", "
    + COLUMN_SQN_UPDATE_USER + ", "
    + COLUMN_SQN_UPDATE_DATE
    + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  // DML: Insert new connection inputs
  public static final String STMT_INSERT_CONNECTION_INPUT =
    "INSERT INTO " + TABLE_SQ_CONNECTION_INPUT + " ("
    + COLUMN_SQNI_CONNECTION + ", "
    + COLUMN_SQNI_INPUT + ", "
    + COLUMN_SQNI_VALUE
    + ") VALUES (?, ?, ?)";

  // DML: Update connection
  public static final String STMT_UPDATE_CONNECTION =
    "UPDATE " + TABLE_SQ_CONNECTION + " SET "
    + COLUMN_SQN_NAME + " = ?, "
    + COLUMN_SQN_UPDATE_USER + " = ?, "
    + COLUMN_SQN_UPDATE_DATE + " = ? "
    + " WHERE " + COLUMN_SQN_ID + " = ?";

  // DML: Enable or disable connection
  public static final String STMT_ENABLE_CONNECTION =
    "UPDATE " + TABLE_SQ_CONNECTION + " SET "
    + COLUMN_SQN_ENABLED + " = ? "
    + " WHERE " + COLUMN_SQN_ID + " = ?";

  // DML: Delete rows from connection input table
  public static final String STMT_DELETE_CONNECTION_INPUT =
    "DELETE FROM " + TABLE_SQ_CONNECTION_INPUT
    + " WHERE " + COLUMN_SQNI_CONNECTION + " = ?";

  // DML: Delete row from connection table
  public static final String STMT_DELETE_CONNECTION =
    "DELETE FROM " + TABLE_SQ_CONNECTION
    + " WHERE " + COLUMN_SQN_ID + " = ?";

  // DML: Select one specific connection
  public static final String STMT_SELECT_CONNECTION_SINGLE =
    "SELECT "
    + COLUMN_SQN_ID + ", "
    + COLUMN_SQN_NAME + ", "
    + COLUMN_SQN_CONNECTOR + ", "
    + COLUMN_SQN_ENABLED + ", "
    + COLUMN_SQN_CREATION_USER + ", "
    + COLUMN_SQN_CREATION_DATE + ", "
    + COLUMN_SQN_UPDATE_USER + ", "
    + COLUMN_SQN_UPDATE_DATE
    + " FROM " + TABLE_SQ_CONNECTION
    + " WHERE " + COLUMN_SQN_ID + " = ?";

  // DML: Select all connections
  public static final String STMT_SELECT_CONNECTION_ALL =
    "SELECT "
    + COLUMN_SQN_ID + ", "
    + COLUMN_SQN_NAME + ", "
    + COLUMN_SQN_CONNECTOR + ", "
    + COLUMN_SQN_ENABLED + ", "
    + COLUMN_SQN_CREATION_USER + ", "
    + COLUMN_SQN_CREATION_DATE + ", "
    + COLUMN_SQN_UPDATE_USER + ", "
    + COLUMN_SQN_UPDATE_DATE
    + " FROM " + TABLE_SQ_CONNECTION;

  // DML: Select all connections for a specific connector.
  public static final String STMT_SELECT_CONNECTION_FOR_CONNECTOR =
    "SELECT "
    + COLUMN_SQN_ID + ", "
    + COLUMN_SQN_NAME + ", "
    + COLUMN_SQN_CONNECTOR + ", "
    + COLUMN_SQN_ENABLED + ", "
    + COLUMN_SQN_CREATION_USER + ", "
    + COLUMN_SQN_CREATION_DATE + ", "
    + COLUMN_SQN_UPDATE_USER + ", "
    + COLUMN_SQN_UPDATE_DATE
    + " FROM " + TABLE_SQ_CONNECTION
    + " WHERE " + COLUMN_SQN_CONNECTOR + " = ?";

  // DML: Check if given connection exists
  public static final String STMT_SELECT_CONNECTION_CHECK =
    "SELECT count(*) FROM " + TABLE_SQ_CONNECTION
    + " WHERE " + COLUMN_SQN_ID + " = ?";

  // DML: Insert new job
  public static final String STMT_INSERT_JOB =
    "INSERT INTO " + TABLE_SQ_JOB + " ("
    + COLUMN_SQB_NAME + ", "
    + COLUMN_SQB_FROM_CONNECTION + ", "
    + COLUMN_SQB_TO_CONNECTION + ", "
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
  public static final String STMT_SELECT_JOB_CHECK =
    "SELECT count(*) FROM " + TABLE_SQ_JOB
    + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Check if there are jobs for given connection
  public static final String STMT_SELECT_JOBS_FOR_CONNECTION_CHECK =
    "SELECT"
    + " count(*)"
    + " FROM " + TABLE_SQ_JOB
    + " JOIN " + TABLE_SQ_CONNECTION
      + " ON " + COLUMN_SQB_FROM_CONNECTION + " = " + COLUMN_SQN_ID
    + " WHERE " + COLUMN_SQN_ID + " = ? ";

  // DML: Select one specific job
  public static final String STMT_SELECT_JOB_SINGLE =
    "SELECT "
    + "FROM_CONNECTOR." + COLUMN_SQN_CONNECTOR + ", "
    + "TO_CONNECTOR." + COLUMN_SQN_CONNECTOR + ", "
    + "job." + COLUMN_SQB_ID + ", "
    + "job." + COLUMN_SQB_NAME + ", "
    + "job." + COLUMN_SQB_FROM_CONNECTION + ", "
    + "job." + COLUMN_SQB_TO_CONNECTION + ", "
    + "job." + COLUMN_SQB_ENABLED + ", "
    + "job." + COLUMN_SQB_CREATION_USER + ", "
    + "job." + COLUMN_SQB_CREATION_DATE + ", "
    + "job." + COLUMN_SQB_UPDATE_USER + ", "
    + "job." + COLUMN_SQB_UPDATE_DATE
    + " FROM " + TABLE_SQ_JOB + " job"
    + " LEFT JOIN " + TABLE_SQ_CONNECTION
    + " FROM_CONNECTOR ON " + COLUMN_SQB_FROM_CONNECTION + " = FROM_CONNECTOR." + COLUMN_SQN_ID
    + " LEFT JOIN " + TABLE_SQ_CONNECTION
    + " TO_CONNECTOR ON " + COLUMN_SQB_TO_CONNECTION + " = TO_CONNECTOR." + COLUMN_SQN_ID
    + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Select all jobs
  public static final String STMT_SELECT_JOB_ALL =
    "SELECT "
    + "FROM_CONNECTION." + COLUMN_SQN_CONNECTOR + ", "
    + "TO_CONNECTION." + COLUMN_SQN_CONNECTOR + ", "
    + "JOB." + COLUMN_SQB_ID + ", "
    + "JOB." + COLUMN_SQB_NAME + ", "
    + "JOB." + COLUMN_SQB_FROM_CONNECTION + ", "
    + "JOB." + COLUMN_SQB_TO_CONNECTION + ", "
    + "JOB." + COLUMN_SQB_ENABLED + ", "
    + "JOB." + COLUMN_SQB_CREATION_USER + ", "
    + "JOB." + COLUMN_SQB_CREATION_DATE + ", "
    + "JOB." + COLUMN_SQB_UPDATE_USER + ", "
    + "JOB." + COLUMN_SQB_UPDATE_DATE
    + " FROM " + TABLE_SQ_JOB + " JOB"
      + " LEFT JOIN " + TABLE_SQ_CONNECTION + " FROM_CONNECTION"
        + " ON " + COLUMN_SQB_FROM_CONNECTION + " = FROM_CONNECTION." + COLUMN_SQN_ID
      + " LEFT JOIN " + TABLE_SQ_CONNECTION + " TO_CONNECTION"
        + " ON " + COLUMN_SQB_TO_CONNECTION + " = TO_CONNECTION." + COLUMN_SQN_ID;

  // DML: Select all jobs for a Connector
  public static final String STMT_SELECT_ALL_JOBS_FOR_CONNECTOR = STMT_SELECT_JOB_ALL
    + " WHERE FROM_CONNECTION." + COLUMN_SQN_CONNECTOR + " = ? OR TO_CONNECTION." + COLUMN_SQN_CONNECTOR + " = ?";

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

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_REMOVE_EXTRA_FRAMEWORK_FORM =
      "DELETE FROM " + TABLE_SQ_FORM
        + " WHERE " + COLUMN_SQF_NAME + "= ?"
        + " AND " + COLUMN_SQF_DIRECTION + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_FRAMEWORK_INDEX =
      "UPDATE " + TABLE_SQ_FORM + " SET "
        + COLUMN_SQF_INDEX + "= ?"
        + " WHERE " + COLUMN_SQF_NAME + "= ?";

  public static final String QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_COLUMN_SQB_TYPE =
      "ALTER TABLE " + TABLE_SQ_JOB + " DROP COLUMN " + COLUMN_SQB_TYPE;

  private DerbySchemaQuery() {
    // Disable explicit object creation
  }
}
