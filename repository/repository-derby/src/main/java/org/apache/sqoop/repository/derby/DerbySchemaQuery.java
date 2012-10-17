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
 *
 * <p>
 * <strong>SQ_CONNECTOR</strong>: Connector registration.
 * <pre>
 *    +----------------------------+
 *    | SQ_CONNECTOR               |
 *    +----------------------------+
 *    | SQC_ID: BIGINT PK AUTO-GEN |
 *    | SQC_NAME: VARCHAR(64)      |
 *    | SQC_CLASS: VARCHAR(255)    |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_FORM</strong>: Form details.
 * <pre>
 *    +-----------------------------+
 *    | SQ_FORM                     |
 *    +-----------------------------+
 *    | SQF_ID: BIGINT PK AUTO-GEN  |
 *    | SQF_CONNECTOR: BIGINT       | FK SQ_CONNECTOR(SQC_ID),NULL for framework
 *    | SQF_OPERATION: VARCHAR(32)  | "IMPORT"|"EXPORT"|NULL
 *    | SQF_NAME: VARCHAR(64)       |
 *    | SQF_TYPE: VARCHAR(32)       | "CONNECTION"|"JOB"
 *    | SQF_INDEX: SMALLINT         |
 *    +-----------------------------+
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
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONNECTION</strong>: Stored connections
 * <pre>
 *    +----------------------------+
 *    | SQ_CONNECTION              |
 *    +----------------------------+
 *    | SQN_ID: BIGINT PK AUTO-GEN |
 *    | SQN_NAME: VARCHAR(64)      |
 *    | SQN_CONNECTOR: BIGINT      | FK SQ_CONNECTOR(SQC_ID)
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_JOB</strong>: Stored jobs
 * <pre>
 *    +----------------------------+
 *    | SQ_JOB                     |
 *    +----------------------------+
 *    | SQB_ID: BIGINT PK AUTO-GEN |
 *    | SQB_NAME: VARCHAR(64)      |
 *    | SQB_TYPE: VARCHAR(64)      |
 *    | SQB_CONNECTION: BIGINT     | FK SQ_CONNECTION(SQN_ID)
 *    +----------------------------+
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
 */
public final class DerbySchemaQuery {

  // DDL: Create schema
  public static final String QUERY_CREATE_SCHEMA_SQOOP =
      "CREATE SCHEMA " + SCHEMA_SQOOP;

  public static final String QUERY_SYSSCHEMA_SQOOP =
    "SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '"
    + SCHEMA_SQOOP + "'";

  // DDL: Create table SQ_CONNECTOR
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR =
      "CREATE TABLE " + TABLE_SQ_CONNECTOR + " (" + COLUMN_SQC_ID
      + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) "
      + "PRIMARY KEY, " + COLUMN_SQC_NAME + " VARCHAR(64), " + COLUMN_SQC_CLASS
      + " VARCHAR(255))";

  // DDL: Create table SQ_FORM
  public static final String QUERY_CREATE_TABLE_SQ_FORM =
      "CREATE TABLE " + TABLE_SQ_FORM + " (" + COLUMN_SQF_ID
      + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) "
      + "PRIMARY KEY, " + COLUMN_SQF_CONNECTOR + " BIGINT, "
      + COLUMN_SQF_OPERATION + " VARCHAR(32), "
      + COLUMN_SQF_NAME + " VARCHAR(64), " + COLUMN_SQF_TYPE + " VARCHAR(32), "
      + COLUMN_SQF_INDEX + " SMALLINT, " + " FOREIGN KEY ("
      + COLUMN_SQF_CONNECTOR+ ") REFERENCES " + TABLE_SQ_CONNECTOR + " ("
      + COLUMN_SQC_ID + "))";

  // DDL: Create table SQ_INPUT
  public static final String QUERY_CREATE_TABLE_SQ_INPUT =
      "CREATE TABLE " + TABLE_SQ_INPUT + " (" + COLUMN_SQI_ID
      + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) "
      + "PRIMARY KEY, " + COLUMN_SQI_NAME + " VARCHAR(64), "
      + COLUMN_SQI_FORM + " BIGINT, " + COLUMN_SQI_INDEX + " SMALLINT, "
      + COLUMN_SQI_TYPE + " VARCHAR(32), " + COLUMN_SQI_STRMASK + " BOOLEAN, "
      + COLUMN_SQI_STRLENGTH + " SMALLINT, FOREIGN KEY (" + COLUMN_SQI_FORM
      + ") REFERENCES " + TABLE_SQ_FORM + " (" + COLUMN_SQF_ID + "))";

  // DDL: Create table SQ_CONNECTION
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTION =
      "CREATE TABLE " + TABLE_SQ_CONNECTION + " (" + COLUMN_SQN_ID
      + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) "
      + "PRIMARY KEY, " + COLUMN_SQN_CONNECTOR + " BIGINT, " + COLUMN_SQN_NAME
      + " VARCHAR(32), FOREIGN KEY(" + COLUMN_SQN_CONNECTOR + ") REFERENCES "
      + TABLE_SQ_CONNECTOR + " (" + COLUMN_SQC_ID + "))";

  // DDL: Create table SQ_JOB
  public static final String QUERY_CREATE_TABLE_SQ_JOB =
      "CREATE TABLE " + TABLE_SQ_JOB + " (" + COLUMN_SQB_ID
      + " BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) "
      + "PRIMARY KEY, " + COLUMN_SQB_CONNECTION + " BIGINT, " + COLUMN_SQB_NAME
      + " VARCHAR(64), " + COLUMN_SQB_TYPE + " VARCHAR(64), FOREIGN KEY("
      + COLUMN_SQB_CONNECTION + ") REFERENCES " + TABLE_SQ_CONNECTION + " ("
      + COLUMN_SQN_ID + "))";

  // DDL: Create table SQ_CONNECTION_INPUT
  public static final String QUERY_CREATE_TABLE_SQ_CONNECTION_INPUT =
      "CREATE TABLE " + TABLE_SQ_CONNECTION_INPUT + " ("
      + COLUMN_SQNI_CONNECTION + " BIGINT, " + COLUMN_SQNI_INPUT + " BIGINT, "
      + COLUMN_SQNI_VALUE + " LONG VARCHAR, PRIMARY KEY ("
      + COLUMN_SQNI_CONNECTION + ", " + COLUMN_SQNI_INPUT + "), FOREIGN KEY ("
      + COLUMN_SQNI_CONNECTION + ") REFERENCES " + TABLE_SQ_CONNECTION + " ("
      + COLUMN_SQN_ID + "), FOREIGN KEY (" + COLUMN_SQNI_INPUT + ") REFERENCES "
      + TABLE_SQ_INPUT + " (" + COLUMN_SQI_ID + "))";

  // DDL: Create table SQ_JOB_INPUT
  public static final String QUERY_CREATE_TABLE_SQ_JOB_INPUT =
      "CREATE TABLE " + TABLE_SQ_JOB_INPUT + " ("
      + COLUMN_SQBI_JOB + " BIGINT, " + COLUMN_SQBI_INPUT + " BIGINT, "
      + COLUMN_SQBI_VALUE + " LONG VARCHAR, PRIMARY KEY ("
      + COLUMN_SQBI_JOB + ", " + COLUMN_SQBI_INPUT + "), FOREIGN KEY ("
      + COLUMN_SQBI_JOB + ") REFERENCES " + TABLE_SQ_JOB + " ("
      + COLUMN_SQB_ID + "), FOREIGN KEY (" + COLUMN_SQBI_INPUT + ") REFERENCES "
      + TABLE_SQ_INPUT + " (" + COLUMN_SQI_ID + "))";

  // DML: Fetch connector Given Name
  public static final String STMT_FETCH_BASE_CONNECTOR =
      "SELECT " + COLUMN_SQC_ID + ", " + COLUMN_SQC_NAME + ", "
      + COLUMN_SQC_CLASS + " FROM " + TABLE_SQ_CONNECTOR + " WHERE "
      + COLUMN_SQC_NAME + " = ?";


  // DML: Fetch all forms for a given connector
  public static final String STMT_FETCH_FORM_CONNECTOR =
      "SELECT " + COLUMN_SQF_ID + ", " + COLUMN_SQF_CONNECTOR + ", "
      + COLUMN_SQF_OPERATION + ", " + COLUMN_SQF_NAME + ", " + COLUMN_SQF_TYPE
      + ", " + COLUMN_SQF_INDEX + " FROM " + TABLE_SQ_FORM + " WHERE "
      + COLUMN_SQF_CONNECTOR + " = ? ORDER BY " + COLUMN_SQF_INDEX;

  // DML: Fetch all framework forms
  public static final String STMT_FETCH_FORM_FRAMEWORK =
      "SELECT " + COLUMN_SQF_ID + ", " + COLUMN_SQF_CONNECTOR + ", "
      + COLUMN_SQF_OPERATION + ", " + COLUMN_SQF_NAME + ", " + COLUMN_SQF_TYPE
      + ", " + COLUMN_SQF_INDEX + " FROM " + TABLE_SQ_FORM + " WHERE " +
      COLUMN_SQF_CONNECTOR + " IS NULL ORDER BY " + COLUMN_SQF_INDEX;

  // DML: Fetch inputs for a given form
  public static final String STMT_FETCH_INPUT =
      "SELECT " + COLUMN_SQI_ID + ", " + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", " + COLUMN_SQI_INDEX + ", " + COLUMN_SQI_TYPE
      + ", " + COLUMN_SQI_STRMASK + ", " + COLUMN_SQI_STRLENGTH
      + ", cast(null as varchar(100)) FROM " + TABLE_SQ_INPUT + " WHERE "
      + COLUMN_SQI_FORM + " = ? ORDER BY " + COLUMN_SQI_INDEX;

  // DML: Fetch inputs and values for a given connection
  public static final String STMT_FETCH_CONNECTION_INPUT =
      "SELECT " + COLUMN_SQI_ID + ", " + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", " + COLUMN_SQI_INDEX + ", " + COLUMN_SQI_TYPE
      + ", " + COLUMN_SQI_STRMASK + ", " + COLUMN_SQI_STRLENGTH
      + ", " + COLUMN_SQNI_VALUE + " FROM " + TABLE_SQ_INPUT
      + " LEFT OUTER JOIN " + TABLE_SQ_CONNECTION_INPUT + " ON "
      + COLUMN_SQNI_INPUT + " = " + COLUMN_SQI_ID + " AND "
      + COLUMN_SQNI_CONNECTION + " = ? WHERE " + COLUMN_SQI_FORM + " = ? AND ("
      + COLUMN_SQNI_CONNECTION + " = ? OR " + COLUMN_SQNI_CONNECTION
      + " IS NULL) ORDER BY " + COLUMN_SQI_INDEX;

  // DML: Fetch inputs and values for a given job
  public static final String STMT_FETCH_JOB_INPUT =
      "SELECT " + COLUMN_SQI_ID + ", " + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", " + COLUMN_SQI_INDEX + ", " + COLUMN_SQI_TYPE
      + ", " + COLUMN_SQI_STRMASK + ", " + COLUMN_SQI_STRLENGTH
      + ", " + COLUMN_SQBI_VALUE + " FROM " + TABLE_SQ_INPUT
      + " LEFT OUTER JOIN " + TABLE_SQ_JOB_INPUT + " ON "
      + COLUMN_SQBI_INPUT + " = " + COLUMN_SQI_ID + " AND  " + COLUMN_SQBI_JOB
      + " = ? WHERE " + COLUMN_SQI_FORM + " = ? AND (" + COLUMN_SQBI_JOB
      + " = ? OR " + COLUMN_SQBI_JOB + " IS NULL) ORDER BY "
      + COLUMN_SQI_INDEX;

  // DML: Insert connector base
  public static final String STMT_INSERT_CONNECTOR_BASE =
      "INSERT INTO " + TABLE_SQ_CONNECTOR + " (" + COLUMN_SQC_NAME
      + ", " + COLUMN_SQC_CLASS + ") VALUES ( ?, ?)";

  // DML: Insert form base
  public static final String STMT_INSERT_FORM_BASE =
      "INSERT INTO " + TABLE_SQ_FORM + " (" + COLUMN_SQF_CONNECTOR + ", "
      + COLUMN_SQF_OPERATION + ", " + COLUMN_SQF_NAME + ", " + COLUMN_SQF_TYPE
      + ", " + COLUMN_SQF_INDEX + ") VALUES ( ?, ?, ?, ?, ?)";

  // DML: Insert form input
  public static final String STMT_INSERT_INPUT_BASE =
      "INSERT INTO " + TABLE_SQ_INPUT + " (" + COLUMN_SQI_NAME + ", "
      + COLUMN_SQI_FORM + ", " + COLUMN_SQI_INDEX + ", " + COLUMN_SQI_TYPE
      + ", " + COLUMN_SQI_STRMASK + ", " + COLUMN_SQI_STRLENGTH + ") "
      + "VALUES (?, ?, ?, ?, ?, ?)";

  // DML: Insert new connection
  public static final String STMT_INSERT_CONNECTION =
    "INSERT INTO " + TABLE_SQ_CONNECTION + " (" + COLUMN_SQN_NAME + ", "
    + COLUMN_SQN_CONNECTOR + ") VALUES (?, ?)";

  // DML: Insert new connection inputs
  public static final String STMT_INSERT_CONNECTION_INPUT =
    "INSERT INTO " + TABLE_SQ_CONNECTION_INPUT + " (" + COLUMN_SQNI_CONNECTION
    + ", " + COLUMN_SQNI_INPUT + ", " + COLUMN_SQNI_VALUE + ") "
    + "VALUES (?, ?, ?)";

  public static final String STMT_UPDATE_CONNECTION =
    "UPDATE " + TABLE_SQ_CONNECTION + " SET " + COLUMN_SQN_NAME + " = ? WHERE "
    + COLUMN_SQN_ID + " = ?";

  // DML: Delete rows from connection input table
  public static final String STMT_DELETE_CONNECTION_INPUT =
    "DELETE FROM " + TABLE_SQ_CONNECTION_INPUT + " WHERE "
    + COLUMN_SQNI_CONNECTION + " = ?";

  // DML: Delete row from connection table
  public static final String STMT_DELETE_CONNECTION =
    "DELETE FROM " + TABLE_SQ_CONNECTION + " WHERE " + COLUMN_SQN_ID + " = ?";

  // DML: Select one specific connection
  public static final String STMT_SELECT_CONNECTION_SINGLE =
    "SELECT " + COLUMN_SQN_ID + ", " + COLUMN_SQN_NAME + ", "
    + COLUMN_SQN_CONNECTOR + " FROM " + TABLE_SQ_CONNECTION + " WHERE "
    + COLUMN_SQN_ID + " = ?";

  // DML: Select all connections
  public static final String STMT_SELECT_CONNECTION_ALL =
    "SELECT " + COLUMN_SQN_ID + ", " + COLUMN_SQN_NAME + ", "
      + COLUMN_SQN_CONNECTOR + " FROM " + TABLE_SQ_CONNECTION;

  // DML: Check if given connection exists
  public static final String STMT_SELECT_CONNECTION_CHECK =
    "SELECT count(*) FROM " + TABLE_SQ_CONNECTION + " WHERE " + COLUMN_SQN_ID
    + " = ?";

  // DML: Insert new job
  public static final String STMT_INSERT_JOB =
    "INSERT INTO " + TABLE_SQ_JOB + " (" + COLUMN_SQB_NAME + ", "
      + COLUMN_SQB_CONNECTION + ", " + COLUMN_SQB_TYPE + ") VALUES (?, ?, ?)";

  // DML: Insert new job inputs
  public static final String STMT_INSERT_JOB_INPUT =
    "INSERT INTO " + TABLE_SQ_JOB_INPUT + " (" + COLUMN_SQBI_JOB
    + ", " + COLUMN_SQBI_INPUT + ", " + COLUMN_SQBI_VALUE + ") "
    + "VALUES (?, ?, ?)";

  public static final String STMT_UPDATE_JOB =
    "UPDATE " + TABLE_SQ_JOB + " SET " + COLUMN_SQB_NAME + " = ? WHERE "
      + COLUMN_SQB_ID + " = ?";

  // DML: Delete rows from job input table
  public static final String STMT_DELETE_JOB_INPUT =
    "DELETE FROM " + TABLE_SQ_JOB_INPUT + " WHERE " + COLUMN_SQBI_JOB + " = ?";

  // DML: Delete row from job table
  public static final String STMT_DELETE_JOB =
    "DELETE FROM " + TABLE_SQ_JOB + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Check if given job exists
  public static final String STMT_SELECT_JOB_CHECK =
    "SELECT count(*) FROM " + TABLE_SQ_JOB + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Check if there are jobs for given connection
  public static final String STMT_SELECT_JOBS_FOR_CONNECTION_CHECK =
    "SELECT count(*) FROM " + TABLE_SQ_JOB + " JOIN " + TABLE_SQ_CONNECTION
    + " ON " + COLUMN_SQB_CONNECTION + " = " + COLUMN_SQN_ID + " WHERE "
    + COLUMN_SQN_ID + " = ? ";

  // DML: Select one specific job
  public static final String STMT_SELECT_JOB_SINGLE =
    "SELECT " + COLUMN_SQN_CONNECTOR + ", " + COLUMN_SQB_ID + ", "
    + COLUMN_SQB_NAME + ", " + COLUMN_SQB_CONNECTION + ", " + COLUMN_SQB_TYPE
    + " FROM " + TABLE_SQ_JOB + " LEFT JOIN " + TABLE_SQ_CONNECTION + " ON "
    + COLUMN_SQB_CONNECTION + " = " + COLUMN_SQN_ID    + " WHERE "
    + COLUMN_SQB_ID + " = ?";

  // DML: Select all jobs
  public static final String STMT_SELECT_JOB_ALL =
    "SELECT " + COLUMN_SQN_CONNECTOR + ", " + COLUMN_SQB_ID + ", "
    + COLUMN_SQB_NAME + ", " + COLUMN_SQB_CONNECTION + ", " + COLUMN_SQB_TYPE
    + " FROM " + TABLE_SQ_JOB + " LEFT JOIN " + TABLE_SQ_CONNECTION + " ON "
    + COLUMN_SQB_CONNECTION + " = " + COLUMN_SQN_ID;

  private DerbySchemaQuery() {
    // Disable explicit object creation
  }
}
