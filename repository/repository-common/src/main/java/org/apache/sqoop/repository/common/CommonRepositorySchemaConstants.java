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
package org.apache.sqoop.repository.common;

// NOTE: This contains and should only contain common repo constants
// PLEASE DO NOT ADD SPECIFIC REPO CONSTANTS HERE
public final class CommonRepositorySchemaConstants {

  public static final String SCHEMA_SQOOP = "SQOOP";

  public static final String SCHEMA_PREFIX = SCHEMA_SQOOP + ".";

  public static final String CONSTRAINT_PREFIX = "FK_";

  // SQ_SYSTEM

  public static final String TABLE_SQ_SYSTEM_NAME = "SQ_SYSTEM";

  public static final String COLUMN_SQM_ID = "SQM_ID";

  public static final String COLUMN_SQM_KEY = "SQM_KEY";

  public static final String COLUMN_SQM_VALUE = "SQM_VALUE";

  // SQ_DIRECTION

  public static final String TABLE_SQ_DIRECTION_NAME = "SQ_DIRECTION";

  public static final String COLUMN_SQD_ID = "SQD_ID";

  public static final String COLUMN_SQD_NAME = "SQD_NAME";

  // SQ_CONFIGURABLE

  public static final String TABLE_SQ_CONFIGURABLE_NAME = "SQ_CONFIGURABLE";

  public static final String COLUMN_SQC_ID = "SQC_ID";

  public static final String COLUMN_SQC_NAME = "SQC_NAME";

  public static final String COLUMN_SQC_CLASS = "SQC_CLASS";

  public static final String COLUMN_SQC_VERSION = "SQC_VERSION";

  public static final String COLUMN_SQC_TYPE = "SQC_TYPE";

  // SQ_CONNECTOR_DIRECTIONS

  public static final String TABLE_SQ_CONNECTOR_DIRECTIONS_NAME = "SQ_CONNECTOR_DIRECTIONS";

  public static final String COLUMN_SQCD_ID = "SQCD_ID";

  public static final String COLUMN_SQCD_CONNECTOR = "SQCD_CONNECTOR";

  public static final String COLUMN_SQCD_DIRECTION = "SQCD_DIRECTION";

  // SQ_CONFIG

  public static final String TABLE_SQ_CONFIG_NAME = "SQ_CONFIG";

  public static final String COLUMN_SQ_CFG_ID = "SQ_CFG_ID";

  public static final String COLUMN_SQ_CFG_CONFIGURABLE = "SQ_CFG_CONFIGURABLE";

  @Deprecated
  public static final String COLUMN_SQ_CFG_DIRECTION = "SQ_CFG_DIRECTION";

  public static final String COLUMN_SQ_CFG_NAME = "SQ_CFG_NAME";

  public static final String COLUMN_SQ_CFG_TYPE = "SQ_CFG_TYPE";

  public static final String COLUMN_SQ_CFG_INDEX = "SQ_CFG_INDEX";

  // SQ_CONFIG_DIRECTIONS

  public static final String TABLE_SQ_CONFIG_DIRECTIONS_NAME = "SQ_CONFIG_DIRECTIONS";

  public static final String COLUMN_SQ_CFG_DIR_ID = "SQ_CFG_DIR_ID";

  public static final String COLUMN_SQ_CFG_DIR_CONFIG = "SQ_CFG_DIR_CONFIG";

  public static final String COLUMN_SQ_CFG_DIR_DIRECTION = "SQ_CFG_DIR_DIRECTION";

  // SQ_INPUT

  public static final String TABLE_SQ_INPUT_NAME = "SQ_INPUT";

  public static final String COLUMN_SQI_ID = "SQI_ID";

  public static final String COLUMN_SQI_NAME = "SQI_NAME";

  public static final String COLUMN_SQI_CONFIG = "SQI_CONFIG";

  public static final String COLUMN_SQI_INDEX = "SQI_INDEX";

  public static final String COLUMN_SQI_TYPE = "SQI_TYPE";

  public static final String COLUMN_SQI_STRMASK = "SQI_STRMASK";

  public static final String COLUMN_SQI_STRLENGTH = "SQI_STRLENGTH";

  public static final String COLUMN_SQI_ENUMVALS = "SQI_ENUMVALS";

  public static final String COLUMN_SQI_EDITABLE = "SQI_EDITABLE";

  // SQ_INPUT_RELATION

  public static final String TABLE_SQ_INPUT_RELATION_NAME = "SQ_INPUT_RELATION";

  public static final String COLUMN_SQIR_ID = "SQIR_ID";

  public static final String COLUMN_SQIR_PARENT = "SQIR_PARENT_ID";

  public static final String COLUMN_SQIR_CHILD = "SQIR_CHILD_ID";


  public static final String TABLE_SQ_LINK_NAME = "SQ_LINK";

  public static final String COLUMN_SQ_LNK_ID = "SQ_LNK_ID";

  public static final String COLUMN_SQ_LNK_NAME = "SQ_LNK_NAME";

  public static final String COLUMN_SQ_LNK_CONFIGURABLE = "SQ_LNK_CONFIGURABLE";

  public static final String COLUMN_SQ_LNK_CREATION_USER = "SQ_LNK_CREATION_USER";

  public static final String COLUMN_SQ_LNK_CREATION_DATE = "SQ_LNK_CREATION_DATE";

  public static final String COLUMN_SQ_LNK_UPDATE_USER = "SQ_LNK_UPDATE_USER";

  public static final String COLUMN_SQ_LNK_UPDATE_DATE = "SQ_LNK_UPDATE_DATE";

  public static final String COLUMN_SQ_LNK_ENABLED = "SQ_LNK_ENABLED";

  // SQ_JOB

  public static final String TABLE_SQ_JOB_NAME = "SQ_JOB";

  public static final String COLUMN_SQB_ID = "SQB_ID";

  public static final String COLUMN_SQB_NAME = "SQB_NAME";

  public static final String COLUMN_SQB_FROM_LINK = "SQB_FROM_LINK";

  public static final String COLUMN_SQB_TO_LINK = "SQB_TO_LINK";

  public static final String COLUMN_SQB_CREATION_USER = "SQB_CREATION_USER";

  public static final String COLUMN_SQB_CREATION_DATE = "SQB_CREATION_DATE";

  public static final String COLUMN_SQB_UPDATE_USER = "SQB_UPDATE_USER";

  public static final String COLUMN_SQB_UPDATE_DATE = "SQB_UPDATE_DATE";

  public static final String COLUMN_SQB_ENABLED = "SQB_ENABLED";

  // SQ_LINK_INPUT

  public static final String TABLE_SQ_LINK_INPUT_NAME = "SQ_LINK_INPUT";

  public static final String COLUMN_SQ_LNKI_LINK = "SQ_LNKI_LINK";

  public static final String COLUMN_SQ_LNKI_INPUT = "SQ_LNKI_INPUT";

  public static final String COLUMN_SQ_LNKI_VALUE = "SQ_LNKI_VALUE";

  // SQ_JOB_INPUT

  public static final String TABLE_SQ_JOB_INPUT_NAME = "SQ_JOB_INPUT";

  public static final String COLUMN_SQBI_JOB = "SQBI_JOB";

  public static final String COLUMN_SQBI_INPUT = "SQBI_INPUT";

  public static final String COLUMN_SQBI_VALUE = "SQBI_VALUE";

  // SQ_SUBMISSION

  public static final String TABLE_SQ_SUBMISSION_NAME = "SQ_SUBMISSION";

  public static final String COLUMN_SQS_ID = "SQS_ID";

  public static final String COLUMN_SQS_JOB = "SQS_JOB";

  public static final String COLUMN_SQS_STATUS = "SQS_STATUS";

  public static final String COLUMN_SQS_CREATION_USER = "SQS_CREATION_USER";

  public static final String COLUMN_SQS_CREATION_DATE = "SQS_CREATION_DATE";

  public static final String COLUMN_SQS_UPDATE_USER = "SQS_UPDATE_USER";

  public static final String COLUMN_SQS_UPDATE_DATE = "SQS_UPDATE_DATE";

  public static final String COLUMN_SQS_EXTERNAL_ID = "SQS_EXTERNAL_ID";

  public static final String COLUMN_SQS_EXTERNAL_LINK = "SQS_EXTERNAL_LINK";

  public static final String COLUMN_SQS_ERROR_SUMMARY = "SQS_ERROR_SUMMARY";

  public static final String COLUMN_SQS_ERROR_DETAILS = "SQS_ERROR_DETAILS";

  // SQ_COUNTER_GROUP

  public static final String TABLE_SQ_COUNTER_GROUP_NAME = "SQ_COUNTER_GROUP";

  public static final String COLUMN_SQG_ID = "SQG_ID";

  public static final String COLUMN_SQG_NAME = "SQG_NAME";

  // SQ_COUNTER_GROUP

  public static final String TABLE_SQ_COUNTER_NAME = "SQ_COUNTER";

  public static final String COLUMN_SQR_ID = "SQR_ID";

  public static final String COLUMN_SQR_NAME = "SQR_NAME";

  // SQ_COUNTER_SUBMISSION

  public static final String TABLE_SQ_COUNTER_SUBMISSION_NAME = "SQ_COUNTER_SUBMISSION";

  public static final String COLUMN_SQRS_GROUP = "SQRS_GROUP";

  public static final String COLUMN_SQRS_COUNTER = "SQRS_COUNTER";

  public static final String COLUMN_SQRS_SUBMISSION = "SQRS_SUBMISSION";

  public static final String COLUMN_SQRS_VALUE = "SQRS_VALUE";

  // Constraints

  // Constraints

  public static final String CONSTRAINT_SQCD_SQC_NAME
      = CONSTRAINT_PREFIX + "SQCD_SQC";

  public static final String CONSTRAINT_SQCD_SQD_NAME
      = CONSTRAINT_PREFIX + "SQCD_SQD";

  public static final String CONSTRAINT_SQ_CONFIGURABLE_UNIQUE_NAME
      = CONSTRAINT_PREFIX + "SQC_NAME_UNIQUE";

  public static final String CONSTRAINT_SQ_CFG_DIR_CONFIG_NAME
      = CONSTRAINT_PREFIX + "SQ_CFG_DIR_CONFIG";

  public static final String CONSTRAINT_SQ_CFG_DIR_DIRECTION_NAME
      = CONSTRAINT_PREFIX + "SQ_CFG_DIR_DIRECTION";

  public static final String CONSTRAINT_SQ_CFG_SQC_NAME
      = CONSTRAINT_PREFIX + "SQ_CFG_SQC";

  public static final String CONSTRAINT_SQ_CONFIG_UNIQUE_NAME_TYPE_CONFIGURABLE
      = CONSTRAINT_PREFIX + "SQ_CFG_NAME_TYPE_CONFIGURABLE_UNIQUE";

  public static final String CONSTRAINT_SQI_SQ_CFG_NAME
      = CONSTRAINT_PREFIX + "SQI_SQ_CFG";

  public static final String CONSTRAINT_SQ_INPUT_UNIQUE_NAME_TYPE_CONFIG
      = CONSTRAINT_PREFIX + "SQI_NAME_TYPE_CONFIG_UNIQUE";

  public static final String CONSTRAINT_SQIR_PARENT_NAME
      = CONSTRAINT_PREFIX + "SQIR_PARENT_ID";

  public static final String CONSTRAINT_SQIR_CHILD_NAME
      = CONSTRAINT_PREFIX + "SQIR_CHILD_ID";

  public static final String CONSTRAINT_SQ_LNK_SQC_NAME
      = CONSTRAINT_PREFIX + "SQ_LNK_SQC";

  public static final String CONSTRAINT_SQ_LNK_NAME_UNIQUE_NAME
      = CONSTRAINT_PREFIX + "SQ_LNK_NAME_UNIQUE";

  public static final String CONSTRAINT_SQB_SQ_LNK_FROM_NAME
      = CONSTRAINT_PREFIX + "SQB_SQ_LNK_FROM";

  public static final String CONSTRAINT_SQB_SQ_LNK_TO_NAME
      = CONSTRAINT_PREFIX + "SQB_SQ_LNK_TO";

  public static final String CONSTRAINT_SQB_NAME_UNIQUE_NAME
      = CONSTRAINT_PREFIX + "SQB_NAME_UNIQUE";

  public static final String CONSTRAINT_SQ_LNKI_SQ_LNK_NAME
      = CONSTRAINT_PREFIX + "SQ_LNKI_SQ_LNK";

  public static final String CONSTRAINT_SQ_LNKI_SQI_NAME
      = CONSTRAINT_PREFIX + "SQ_LNKI_SQI";

  public static final String CONSTRAINT_SQBI_SQB_NAME
      = CONSTRAINT_PREFIX + "SQBI_SQB";

  public static final String CONSTRAINT_SQBI_SQI_NAME
      = CONSTRAINT_PREFIX + "SQBI_SQI";

  public static final String CONSTRAINT_SQS_SQB_NAME
      = CONSTRAINT_PREFIX + "SQS_SQB";

  public static final String CONSTRAINT_SQRS_SQG_NAME
      = CONSTRAINT_PREFIX + "SQRS_SQG";

  public static final String CONSTRAINT_SQRS_SQR_NAME
      = CONSTRAINT_PREFIX + "SQRS_SQR";

  public static final String CONSTRAINT_SQRS_SQS_NAME
      = CONSTRAINT_PREFIX + "SQRS_SQS";

  private CommonRepositorySchemaConstants() {
    // Disable explicit object creation
  }
}