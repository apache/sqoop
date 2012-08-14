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
 */
public final class DerbySchemaQuery {

  // DDL: Create schema
  public static final String QUERY_CREATE_SCHEMA_SQOOP =
      "CREATE SCHEMA " + SCHEMA_SQOOP;

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
      + ", " + COLUMN_SQI_STRMASK + ", " + COLUMN_SQI_STRLENGTH + " FROM "
      + TABLE_SQ_INPUT + " WHERE " + COLUMN_SQI_FORM + " = ? ORDER BY "
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

  private DerbySchemaQuery() {
    // Disable explicit object creation
  }
}
