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
package org.apache.sqoop.repository.postgresql;

import static org.apache.sqoop.repository.postgresql.PostgresqlSchemaConstants.*;

/**
 * DDL queries that create the Sqoop repository schema in PostgreSQL database. These
 * queries create the following tables:
 * <p>
 * <strong>SQ_SYSTEM</strong>: Store for various state information
 * <pre>
 *    +----------------------------+
 *    | SQ_SYSTEM                  |
 *    +----------------------------+
 *    | SQM_ID: BIGSERIAL PK       |
 *    | SQM_KEY: VARCHAR(64)       |
 *    | SQM_VALUE: VARCHAR(64)     |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_DIRECTION</strong>: Directions.
 * <pre>
 *    +--------------------------+
 *    | SQ_DIRECTION             |
 *    +--------------------------+
 *    | SQD_ID: BIGSERIAL PK     |
 *    | SQD_NAME: VARCHAR(64)    | "FROM"|"TO"
 *    +--------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONFIGURABLE</strong>: Configurable registration.
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
 *    | SQCD_ID: BIGSERIAL PK        |
 *    | SQCD_CONNECTOR: BIGINT       | FK SQCD_CONNECTOR(SQC_ID)
 *    | SQCD_DIRECTION: BIGINT       | FK SQCD_DIRECTION(SQD_ID)
 *    +------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONFIG</strong>: Config details.
 * <pre>
 *    +-------------------------------------+
 *    | SQ_CONFIG                           |
 *    +-------------------------------------+
 *    | SQ_CFG_ID: BIGSERIAL PK             |
 *    | SQ_CFG_CONNECTOR: BIGINT            | FK SQ_CFG_CONNECTOR(SQC_ID),NULL for driver
 *    | SQ_CFG_NAME: VARCHAR(64)            |
 *    | SQ_CFG_TYPE: VARCHAR(32)            | "LINK"|"JOB"
 *    | SQ_CFG_INDEX: SMALLINT              |
 *    +-------------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_CONFIG_DIRECTIONS</strong>: Connector directions.
 * <pre>
 *    +------------------------------+
 *    | SQ_CONFIG_DIRECTIONS         |
 *    +------------------------------+
 *    | SQ_CFG_ID: BIGSERIAL PK      |
 *    | SQ_CFG_DIR_CONFIG: BIGINT    | FK SQ_CFG_DIR_CONFIG(SQ_CFG_ID)
 *    | SQ_CFG_DIR_DIRECTION: BIGINT | FK SQ_CFG_DIR_DIRECTION(SQD_ID)
 *    +------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_INPUT</strong>: Input details
 * <pre>
 *    +----------------------------+
 *    | SQ_INPUT                   |
 *    +----------------------------+
 *    | SQI_ID: BIGSERIAL PK       |
 *    | SQI_NAME: VARCHAR(64)      |
 *    | SQI_CONFIG: BIGINT         | FK SQI_CONFIG(SQ_CFG_ID)
 *    | SQI_INDEX: SMALLINT        |
 *    | SQI_TYPE: VARCHAR(32)      | "STRING"|"MAP"
 *    | SQI_STRMASK: BOOLEAN       |
 *    | SQI_STRLENGTH: SMALLINT    |
 *    | SQI_ENUMVALS: VARCHAR(100) |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_LINK</strong>: Stored connections
 * <pre>
 *    +-----------------------------------+
 *    | SQ_LINK                           |
 *    +-----------------------------------+
 *    | SQ_LNK_ID: BIGSERIAL PK           |
 *    | SQ_LNK_NAME: VARCHAR(64)          |
 *    | SQ_LNK_CONNECTOR: BIGINT          | FK SQ_CONNECTOR(SQC_ID)
 *    | SQ_LNK_CREATION_USER: VARCHAR(32) |
 *    | SQ_LNK_CREATION_DATE: TIMESTAMP   |
 *    | SQ_LNK_UPDATE_USER: VARCHAR(32)   |
 *    | SQ_LNK_UPDATE_DATE: TIMESTAMP     |
 *    | SQ_LNK_ENABLED: BOOLEAN           |
 *    +-----------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_JOB</strong>: Stored jobs
 * <pre>
 *    +--------------------------------+
 *    | SQ_JOB                         |
 *    +--------------------------------+
 *    | SQB_ID: BIGSERIAL PK           |
 *    | SQB_NAME: VARCHAR(64)          |
 *    | SQB_FROM_LINK: BIGINT          | FK SQ_LINK(SQ_LNK_ID)
 *    | SQB_TO_LINK: BIGINT            | FK SQ_LINK(SQ_LNK_ID)
 *    | SQB_CREATION_USER: VARCHAR(32) |
 *    | SQB_CREATION_DATE: TIMESTAMP   |
 *    | SQB_UPDATE_USER: VARCHAR(32)   |
 *    | SQB_UPDATE_DATE: TIMESTAMP     |
 *    | SQB_ENABLED: BOOLEAN           |
 *    +--------------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_LINK_INPUT</strong>: N:M relationship link and input
 * <pre>
 *    +----------------------------+
 *    | SQ_LINK_INPUT              |
 *    +----------------------------+
 *    | SQ_LNK_LINK: BIGSERIAL     | FK SQ_LINK(SQ_LNK_ID)
 *    | SQ_LNK_INPUT: BIGINT       | FK SQ_INPUT(SQI_ID)
 *    | SQ_LNK_VALUE: VARCHAR      |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_JOB_INPUT</strong>: N:M relationship job and input
 * <pre>
 *    +----------------------------+
 *    | SQ_JOB_INPUT               |
 *    +----------------------------+
 *    | SQBI_JOB: BIGINT           | FK SQ_JOB(SQB_ID)
 *    | SQBI_INPUT: BIGINT         | FK SQ_INPUT(SQI_ID)
 *    | SQBI_VALUE: VARCHAR(1000)  |
 *    +----------------------------+
 * </pre>
 * </p>
 * <p>
 * <strong>SQ_SUBMISSION</strong>: List of submissions
 * <pre>
 *    +-----------------------------------+
 *    | SQ_JOB_SUBMISSION                 |
 *    +-----------------------------------+
 *    | SQS_ID: BIGSERIAL PK              |
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
public class PostgresqlSchemaCreateQuery {

  public static final String QUERY_CREATE_SCHEMA_SQOOP =
      "CREATE SCHEMA " + SCHEMA_SQOOP;

  public static final String QUERY_CREATE_TABLE_SQ_SYSTEM =
      "CREATE TABLE " + TABLE_SQ_SYSTEM + " ("
          + COLUMN_SQM_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQM_KEY + " VARCHAR(64), "
          + COLUMN_SQM_VALUE + " VARCHAR(64) "
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_DIRECTION =
      "CREATE TABLE " + TABLE_SQ_DIRECTION + " ("
          + COLUMN_SQD_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQD_NAME + " VARCHAR(64)"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONFIGURABLE =
      "CREATE TABLE " + TABLE_SQ_CONFIGURABLE + " ("
          + COLUMN_SQC_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQC_NAME + " VARCHAR(64) UNIQUE, "
          + COLUMN_SQC_TYPE + " VARCHAR(32), "
          + COLUMN_SQC_CLASS + " VARCHAR(255), "
          + COLUMN_SQC_VERSION + " VARCHAR(64) "
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS =
      "CREATE TABLE " + TABLE_SQ_CONNECTOR_DIRECTIONS + " ("
          + COLUMN_SQCD_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQCD_CONNECTOR + " BIGINT REFERENCES " + TABLE_SQ_CONFIGURABLE + "("  + COLUMN_SQC_ID + ")" + ", "
          + COLUMN_SQCD_DIRECTION + " BIGINT REFERENCES " + TABLE_SQ_DIRECTION + "("  + COLUMN_SQD_ID + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONFIG =
      "CREATE TABLE " + TABLE_SQ_CONFIG + " ("
          + COLUMN_SQ_CFG_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQ_CFG_CONFIGURABLE + " BIGINT REFERENCES " + TABLE_SQ_CONFIGURABLE + "("  + COLUMN_SQC_ID + ")" + ", "
          + COLUMN_SQ_CFG_NAME + " VARCHAR(64), "
          + COLUMN_SQ_CFG_TYPE + " VARCHAR(32), "
          + COLUMN_SQ_CFG_INDEX + " SMALLINT, "
          + "UNIQUE (" + COLUMN_SQ_CFG_NAME + ", " + COLUMN_SQ_CFG_TYPE + ", " + COLUMN_SQ_CFG_CONFIGURABLE + ") "
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS =
      "CREATE TABLE " + TABLE_SQ_CONFIG_DIRECTIONS + " ("
          + COLUMN_SQ_CFG_DIR_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQ_CFG_DIR_CONFIG + " BIGINT REFERENCES " + TABLE_SQ_CONFIG + "("  + COLUMN_SQ_CFG_ID + ")" + ", "
          + COLUMN_SQ_CFG_DIR_DIRECTION + " BIGINT REFERENCES " + TABLE_SQ_DIRECTION + "("  + COLUMN_SQD_ID + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_INPUT =
      "CREATE TABLE " + TABLE_SQ_INPUT + " ("
          + COLUMN_SQI_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQI_NAME + " VARCHAR(64), "
          + COLUMN_SQI_CONFIG + " BIGINT REFERENCES " + TABLE_SQ_CONFIG + "("  + COLUMN_SQ_CFG_ID + ")" + ", "
          + COLUMN_SQI_INDEX + " SMALLINT, "
          + COLUMN_SQI_TYPE + " VARCHAR(32), "
          + COLUMN_SQI_STRMASK + " BOOLEAN, "
          + COLUMN_SQI_STRLENGTH + " SMALLINT, "
          + COLUMN_SQI_ENUMVALS + " VARCHAR(100), "
          + " UNIQUE (" + COLUMN_SQI_NAME + ", " + COLUMN_SQI_TYPE + ", " + COLUMN_SQI_CONFIG + ") "
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_LINK =
      "CREATE TABLE " + TABLE_SQ_LINK + " ("
          + COLUMN_SQ_LNK_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQ_LNK_CONFIGURABLE + " BIGINT REFERENCES " + TABLE_SQ_CONFIGURABLE + "("  + COLUMN_SQC_ID + ")" + ", "
          + COLUMN_SQ_LNK_NAME  + " VARCHAR(32) UNIQUE, "
          + COLUMN_SQ_LNK_CREATION_DATE + " TIMESTAMP, "
          + COLUMN_SQ_LNK_CREATION_USER + " VARCHAR(32) DEFAULT NULL, "
          + COLUMN_SQ_LNK_UPDATE_DATE + " TIMESTAMP, "
          + COLUMN_SQ_LNK_UPDATE_USER + " VARCHAR(32) DEFAULT NULL, "
          + COLUMN_SQ_LNK_ENABLED + " BOOLEAN DEFAULT TRUE"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_JOB =
      "CREATE TABLE " + TABLE_SQ_JOB + " ("
          + COLUMN_SQB_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQB_FROM_LINK + " BIGINT REFERENCES " + TABLE_SQ_LINK + "("  + COLUMN_SQ_LNK_ID + ")" + ", "
          + COLUMN_SQB_TO_LINK + " BIGINT REFERENCES " + TABLE_SQ_LINK + "("  + COLUMN_SQ_LNK_ID + ")" + ", "
          + COLUMN_SQB_NAME + " VARCHAR(64) UNIQUE, "
          + COLUMN_SQB_CREATION_DATE + " TIMESTAMP, "
          + COLUMN_SQB_CREATION_USER + " VARCHAR(32) DEFAULT NULL, "
          + COLUMN_SQB_UPDATE_DATE + " TIMESTAMP, "
          + COLUMN_SQB_UPDATE_USER + " VARCHAR(32) DEFAULT NULL, "
          + COLUMN_SQB_ENABLED + " BOOLEAN DEFAULT TRUE"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_LINK_INPUT =
      "CREATE TABLE " + TABLE_SQ_LINK_INPUT + " ("
          + COLUMN_SQ_LNKI_LINK + " BIGINT REFERENCES " + TABLE_SQ_LINK + "("  + COLUMN_SQ_LNK_ID + ")" + ", "
          + COLUMN_SQ_LNKI_INPUT + " BIGINT REFERENCES " + TABLE_SQ_INPUT + "("  + COLUMN_SQI_ID + ")" + ", "
          + COLUMN_SQ_LNKI_VALUE + " VARCHAR, "
          + "PRIMARY KEY (" + COLUMN_SQ_LNKI_LINK + ", " + COLUMN_SQ_LNKI_INPUT + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_JOB_INPUT =
      "CREATE TABLE " + TABLE_SQ_JOB_INPUT + " ("
          + COLUMN_SQBI_JOB + " BIGINT REFERENCES " + TABLE_SQ_JOB + "("  + COLUMN_SQB_ID + ")" + ", "
          + COLUMN_SQBI_INPUT + " BIGINT REFERENCES " + TABLE_SQ_INPUT + "("  + COLUMN_SQI_ID + ")" + ", "
          + COLUMN_SQBI_VALUE + " VARCHAR(1000), "
          + "PRIMARY KEY (" + COLUMN_SQBI_JOB + ", " + COLUMN_SQBI_INPUT + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_SUBMISSION =
      "CREATE TABLE " + TABLE_SQ_SUBMISSION + " ("
          + COLUMN_SQS_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQS_JOB + " BIGINT REFERENCES " + TABLE_SQ_JOB + "("  + COLUMN_SQB_ID + ") ON DELETE CASCADE, "
          + COLUMN_SQS_STATUS + " VARCHAR(20), "
          + COLUMN_SQS_CREATION_DATE + " TIMESTAMP, "
          + COLUMN_SQS_CREATION_USER + " VARCHAR(32) DEFAULT NULL, "
          + COLUMN_SQS_UPDATE_DATE + " TIMESTAMP, "
          + COLUMN_SQS_UPDATE_USER + " VARCHAR(32) DEFAULT NULL, "
          + COLUMN_SQS_EXTERNAL_ID + " VARCHAR(50), "
          + COLUMN_SQS_EXTERNAL_LINK + " VARCHAR(150), "
          + COLUMN_SQS_ERROR_SUMMARY + " VARCHAR(150), "
          + COLUMN_SQS_ERROR_DETAILS + " VARCHAR(750)"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_COUNTER_GROUP =
      "CREATE TABLE " + TABLE_SQ_COUNTER_GROUP + " ("
          + COLUMN_SQG_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQG_NAME + " VARCHAR(75) UNIQUE"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_COUNTER =
      "CREATE TABLE " + TABLE_SQ_COUNTER + " ("
          + COLUMN_SQR_ID + " BIGSERIAL PRIMARY KEY NOT NULL, "
          + COLUMN_SQR_NAME + " VARCHAR(75) UNIQUE"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION =
      "CREATE TABLE " + TABLE_SQ_COUNTER_SUBMISSION + " ("
          + COLUMN_SQRS_GROUP + " BIGINT REFERENCES " + TABLE_SQ_COUNTER_GROUP + "("  + COLUMN_SQG_ID + ")" + ", "
          + COLUMN_SQRS_COUNTER + " BIGINT REFERENCES " + TABLE_SQ_COUNTER + "("  + COLUMN_SQR_ID + ")" + ", "
          + COLUMN_SQRS_SUBMISSION + " BIGINT REFERENCES " + TABLE_SQ_SUBMISSION + "("  + COLUMN_SQS_ID + ") ON DELETE CASCADE" + ", "
          + COLUMN_SQRS_VALUE + " BIGINT, "
          + "PRIMARY KEY (" + COLUMN_SQRS_GROUP + ", " + COLUMN_SQRS_COUNTER + ", " + COLUMN_SQRS_SUBMISSION + ")"
          + ")";

  private PostgresqlSchemaCreateQuery() {
    // Disable explicit object creation
  }
}
