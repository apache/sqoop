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
package org.apache.sqoop.repository.mysql;

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.SCHEMA_SQOOP;

import org.apache.sqoop.repository.common.CommonRepoUtils;
import org.apache.sqoop.repository.common.CommonRepositorySchemaConstants;

public class MySqlSchemaCreateQuery {

  public static final String QUERY_CREATE_DATABASE_SQOOP = "CREATE DATABASE " + CommonRepoUtils.escapeDatabaseName(SCHEMA_SQOOP);

  public static final String QUERY_CREATE_TABLE_SQ_SYSTEM =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_KEY) + " VARCHAR(64), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQM_VALUE) + " VARCHAR(64) "
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_DIRECTION =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_DIRECTION_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQD_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQD_NAME) + " VARCHAR(64)"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONFIGURABLE =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIGURABLE_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_NAME) + " VARCHAR(64) NOT NULL,"
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_TYPE) + " VARCHAR(32), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_CLASS) + " VARCHAR(255), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_VERSION) + " VARCHAR(64), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_CONFIGURABLE_UNIQUE_NAME)
          + " UNIQUE (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_NAME) + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONNECTOR_DIRECTIONS_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCD_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCD_CONNECTOR) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCD_DIRECTION) + " BIGINT, "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQCD_SQC_NAME)
          + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCD_CONNECTOR) + ") REFERENCES "
          + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIGURABLE_NAME) + "("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_ID) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQCD_SQD_NAME)
          + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCD_DIRECTION) + ") REFERENCES "
          + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_DIRECTION_NAME) + "("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQD_ID) + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONFIG =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIG_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_CONFIGURABLE) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_NAME) + " VARCHAR(64), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_TYPE) + " VARCHAR(32), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_INDEX) + " SMALLINT, "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_CFG_SQC_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_CONFIGURABLE) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIGURABLE_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_ID) + "),"
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_CONFIG_UNIQUE_NAME_TYPE_CONFIGURABLE)
            + " UNIQUE (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_NAME) + ", "
            + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_TYPE) + ", "
            + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_CONFIGURABLE) + ") "
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIG_DIRECTIONS_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_DIR_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_DIR_CONFIG) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_DIR_DIRECTION) + " BIGINT, "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_CFG_DIR_CONFIG_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_DIR_CONFIG) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIG_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_ID) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_CFG_DIR_DIRECTION_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_DIR_DIRECTION) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_DIRECTION_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQD_ID) + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_INPUT =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_INPUT_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_NAME) + " VARCHAR(64), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_CONFIG) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_INDEX) + " SMALLINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_TYPE) + " VARCHAR(32), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_STRMASK) + " BOOLEAN, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_STRLENGTH) + " SMALLINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_EDITABLE) + " VARCHAR(32), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_ENUMVALS) + " VARCHAR(100), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQI_SQ_CFG_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_CONFIG) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIG_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_CFG_ID) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_INPUT_UNIQUE_NAME_TYPE_CONFIG)
            + " UNIQUE (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_NAME) + ", "
                          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_TYPE) + ", "
                          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_CONFIG) + ") "
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_INPUT_RELATION =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_INPUT_RELATION_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQIR_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQIR_PARENT) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQIR_CHILD) + " BIGINT, "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQIR_PARENT_NAME)
            + " FOREIGN KEY (" + CommonRepositorySchemaConstants.COLUMN_SQIR_PARENT + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_INPUT_NAME) + "("  + CommonRepositorySchemaConstants.COLUMN_SQI_ID + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQIR_CHILD_NAME)
            + " FOREIGN KEY (" + CommonRepositorySchemaConstants.COLUMN_SQIR_CHILD + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_INPUT_NAME) + "("  + CommonRepositorySchemaConstants.COLUMN_SQI_ID + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_LINK =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_LINK_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_CONFIGURABLE) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_NAME) + " VARCHAR(32) NOT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_CREATION_DATE) + " TIMESTAMP, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_CREATION_USER) + " VARCHAR(32) DEFAULT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_UPDATE_DATE) + " TIMESTAMP, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_UPDATE_USER) + " VARCHAR(32) DEFAULT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_ENABLED) + " BOOLEAN DEFAULT TRUE, "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_LNK_NAME_UNIQUE_NAME)
          + " UNIQUE (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_NAME) + "),"
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_LNK_SQC_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_CONFIGURABLE) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONFIGURABLE_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQC_ID) + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_JOB =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_JOB_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_FROM_LINK) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_TO_LINK) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_NAME) + " VARCHAR(64) NOT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_CREATION_DATE) + " TIMESTAMP, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_CREATION_USER) + " VARCHAR(32) DEFAULT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_UPDATE_DATE) + " TIMESTAMP, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_UPDATE_USER) + " VARCHAR(32) DEFAULT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_ENABLED) + " BOOLEAN DEFAULT TRUE, "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQB_NAME_UNIQUE_NAME)
          + " UNIQUE (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_NAME) + "),"
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQB_SQ_LNK_FROM_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_FROM_LINK) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_LINK_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_ID) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQB_SQ_LNK_TO_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_TO_LINK) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_LINK_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_ID) + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_LINK_INPUT =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_LINK_INPUT_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_LINK) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_INPUT) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_VALUE) + " VARCHAR(1000), "
          + "PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_LINK) + ", "
            + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_INPUT) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_LNKI_SQ_LNK_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_LINK) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_LINK_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNK_ID) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQ_LNKI_SQI_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_INPUT) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_INPUT_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_ID) + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_JOB_INPUT =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_JOB_INPUT_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQBI_JOB) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQBI_INPUT) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQBI_VALUE) + " VARCHAR(1000), "
          + "PRIMARY KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQBI_JOB) + ", "
            + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQBI_INPUT) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQBI_SQB_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQBI_JOB) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_JOB_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_ID) + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQBI_SQI_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQBI_INPUT) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_INPUT_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQI_ID) + ")"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_SUBMISSION =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SUBMISSION_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_JOB) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_STATUS) + " VARCHAR(20), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_CREATION_DATE) + " TIMESTAMP, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_CREATION_USER) + " VARCHAR(32) DEFAULT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_UPDATE_DATE) + " TIMESTAMP, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_UPDATE_USER) + " VARCHAR(32) DEFAULT NULL, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_EXTERNAL_ID) + " VARCHAR(50), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_EXTERNAL_LINK) + " VARCHAR(150), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_ERROR_SUMMARY) + " VARCHAR(150), "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_ERROR_DETAILS) + " VARCHAR(750), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQS_SQB_NAME)
            + " FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_JOB) + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_JOB_NAME)
              + "("  + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQB_ID) + ") ON DELETE CASCADE"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_COUNTER_GROUP =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_COUNTER_GROUP_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQG_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQG_NAME) + " VARCHAR(75) UNIQUE"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_COUNTER =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_COUNTER_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQR_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQR_NAME) + " VARCHAR(75) UNIQUE"
          + ")";

  public static final String QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION =
      "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_COUNTER_SUBMISSION_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQRS_GROUP) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQRS_COUNTER) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQRS_SUBMISSION) + " BIGINT, "
          + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQRS_VALUE) + " BIGINT, "
          + "PRIMARY KEY (" + CommonRepositorySchemaConstants.COLUMN_SQRS_GROUP + ", " + CommonRepositorySchemaConstants.COLUMN_SQRS_COUNTER + ", " + CommonRepositorySchemaConstants.COLUMN_SQRS_SUBMISSION + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQRS_SQG_NAME)
            + " FOREIGN KEY (" + CommonRepositorySchemaConstants.COLUMN_SQRS_GROUP + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_COUNTER_GROUP_NAME) + "("  + CommonRepositorySchemaConstants.COLUMN_SQG_ID + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQRS_SQR_NAME)
            + " FOREIGN KEY (" + CommonRepositorySchemaConstants.COLUMN_SQRS_COUNTER + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_COUNTER_NAME) + "("  + CommonRepositorySchemaConstants.COLUMN_SQR_ID + "), "
          + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQRS_SQS_NAME)
            + " FOREIGN KEY (" + CommonRepositorySchemaConstants.COLUMN_SQRS_SUBMISSION + ") REFERENCES "
              + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SUBMISSION_NAME) + "("  + CommonRepositorySchemaConstants.COLUMN_SQS_ID + ") ON DELETE CASCADE"
          + ")";

   // DDL: Create table SQ_CONTEXT_TYPE
   public static final String QUERY_CREATE_TABLE_SQ_CONTEXT_TYPE =
       "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONTEXT_TYPE) + " ("
           + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCT_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
           + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCT_NAME) + " VARCHAR(25) UNIQUE"
           + ")";

   // DDL: Create table SQ_CONTEXT_PROPERTY
   public static final String QUERY_CREATE_TABLE_SQ_CONTEXT_PROPERTY =
       "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONTEXT_PROPERTY) + " ("
           + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCP_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
           + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCP_NAME) + " VARCHAR(500) UNIQUE"
           + ")";

   // DDL: Create table SQ_CONTEXT
   public static final String QUERY_CREATE_TABLE_SQ_CONTEXT =
       "CREATE TABLE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONTEXT) + " ("
       + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
       + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_SUBMISSION) + " BIGINT, "
       + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_TYPE) + " BIGINT, "
       + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_PROPERTY) + " BIGINT, "
       + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_VALUE) + " VARCHAR(500), "
       + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQCO_SQS_ID) + " "
         + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_SUBMISSION) + ") "
           + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_SUBMISSION_NAME)
           + "(" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQS_ID) + ") ON DELETE CASCADE, "
       + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQCO_SQCT_ID) + " "
         + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_TYPE) + ") "
           + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONTEXT_TYPE)
           + "(" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCT_ID) + "), "
       + "CONSTRAINT " + CommonRepoUtils.escapeConstraintName(CommonRepositorySchemaConstants.CONSTRAINT_SQCO_SQCP_ID) + " "
         + "FOREIGN KEY (" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCO_PROPERTY) + ") "
           + "REFERENCES " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, CommonRepositorySchemaConstants.TABLE_SQ_CONTEXT_PROPERTY)
           + "(" + CommonRepoUtils.escapeColumnName(CommonRepositorySchemaConstants.COLUMN_SQCP_ID) + ") "
       + ")";

  private MySqlSchemaCreateQuery() {
    // Disable explicit object creation
  }
}
