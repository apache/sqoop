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

import java.util.HashSet;
import java.util.Set;

// NOTE: This contains and should only contain derby specific constants, such as the ones that existed before
// and are relevant to only derby before migrating to the newer names
// PLEASE DO NOT ADD COMMON CONSTANTS HERE
public final class DerbySchemaConstants {

  // SQ_SYSTEM relevant only in derby
  public static final String TABLE_SQ_SYSTEM_NAME = "SQ_SYSTEM";
  public static final String TABLE_SQ_SYSTEM = SCHEMA_PREFIX + TABLE_SQ_SYSTEM_NAME;
  public static final String COLUMN_SQM_ID = "SQM_ID";
  public static final String COLUMN_SQM_KEY = "SQM_KEY";
  public static final String COLUMN_SQM_VALUE = "SQM_VALUE";

  // SQ_CONNECTOR
  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_CONNECTOR_NAME = "SQ_CONNECTOR";

  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_CONNECTOR = SCHEMA_PREFIX + TABLE_SQ_CONNECTOR_NAME;

  // constraints relevant only in derby
  public static final String CONSTRAINT_SQCD_SQC_NAME = CONSTRAINT_PREFIX + "SQCD_SQC";
  // FK to the SQ_CONNECTOR table
  public static final String CONSTRAINT_SQCD_SQC = SCHEMA_PREFIX + CONSTRAINT_SQCD_SQC_NAME;
  public static final String CONSTRAINT_SQCD_SQD_NAME = CONSTRAINT_PREFIX + "SQCD_SQD";
  // FK to the SQ_DIRECTION table
  public static final String CONSTRAINT_SQCD_SQD = SCHEMA_PREFIX + CONSTRAINT_SQCD_SQD_NAME;

  // SQ_CONFIGURABLE
  //constraint only relevant in derby
  public static final String CONSTRAINT_SQ_CONFIGURABLE_UNIQUE_NAME = CONSTRAINT_PREFIX
      + "SQC_NAME_UNIQUE";
  public static final String CONSTRAINT_SQ_CONFIGURABLE_UNIQUE = SCHEMA_PREFIX
      + CONSTRAINT_SQ_CONFIGURABLE_UNIQUE_NAME;

  // SQ_CONFIG_DIRECTION
  // only relevant in derby
  public static final String COLUMN_SQ_CFG_DIR_ID = "SQ_CFG_DIR_ID";

  // SQ_CONFIG
  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_FORM_NAME = "SQ_FORM";

  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_FORM = SCHEMA_PREFIX + TABLE_SQ_FORM_NAME;

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQF_ID = "SQF_ID";

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQF_CONNECTOR = "SQF_CONNECTOR";
  @Deprecated
  // used only for upgrade path
  public static final String COLUMN_SQ_CFG_CONNECTOR = "SQ_CFG_CONNECTOR";

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQF_OPERATION = "SQF_OPERATION";

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQF_DIRECTION = "SQF_DIRECTION";

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQF_NAME = "SQF_NAME";

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQF_TYPE = "SQF_TYPE";

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQF_INDEX = "SQF_INDEX";

  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQF_SQC_NAME = CONSTRAINT_PREFIX + "SQF_SQC";
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQF_SQC = SCHEMA_PREFIX + CONSTRAINT_SQF_SQC_NAME;

  @Deprecated
  // used only for the upgrade code
  public static final String COLUMN_SQ_CFG_DIR_FORM = "SQ_CFG_DIR_FORM";

  // constraint relevant only in derby
  public static final String CONSTRAINT_SQ_CFG_DIR_CONFIG_NAME = CONSTRAINT_PREFIX
      + "SQ_CFG_DIR_CONFIG";

  // this is a FK to the SQ_CONFIG table
  public static final String CONSTRAINT_SQ_CFG_DIR_CONFIG = SCHEMA_PREFIX
      + CONSTRAINT_SQ_CFG_DIR_CONFIG_NAME;

  public static final String CONSTRAINT_SQ_CFG_DIR_DIRECTION_NAME = CONSTRAINT_PREFIX
      + "SQ_CFG_DIR_DIRECTION";

  // this a FK to the SQ_DIRECTION table
  public static final String CONSTRAINT_SQ_CFG_DIR_DIRECTION = SCHEMA_PREFIX
      + CONSTRAINT_SQ_CFG_DIR_DIRECTION_NAME;

  // FK constraint on configurable
  public static final String CONSTRAINT_SQ_CFG_SQC_NAME = CONSTRAINT_PREFIX + "SQ_CFG_SQC";
  public static final String CONSTRAINT_SQ_CFG_SQC = SCHEMA_PREFIX + CONSTRAINT_SQ_CFG_SQC_NAME;

  // uniqueness constraint
  public static final String CONSTRAINT_SQ_CONFIG_UNIQUE_NAME_TYPE_CONFIGURABLE = CONSTRAINT_PREFIX
      + "SQ_CFG_NAME_TYPE_CONFIGURABLE_UNIQUE";
  public static final String CONSTRAINT_SQ_CONFIG_UNIQUE = SCHEMA_PREFIX
      + CONSTRAINT_SQ_CONFIG_UNIQUE_NAME_TYPE_CONFIGURABLE;

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQI_FORM = "SQI_FORM";

  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQI_SQF_NAME = CONSTRAINT_PREFIX + "SQI_SQF";
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQI_SQF = SCHEMA_PREFIX + CONSTRAINT_SQI_SQF_NAME;

  // constraints relevant only in derby

  public static final String CONSTRAINT_SQI_SQ_CFG_NAME = CONSTRAINT_PREFIX + "SQI_SQ_CFG";
  public static final String CONSTRAINT_SQI_SQ_CFG = SCHEMA_PREFIX + CONSTRAINT_SQI_SQ_CFG_NAME;

  // uniqueness constraint
  public static final String CONSTRAINT_SQ_INPUT_UNIQUE_NAME_TYPE_CONFIG = CONSTRAINT_PREFIX
      + "SQI_NAME_TYPE_CONFIG_UNIQUE";
  public static final String CONSTRAINT_SQ_INPUT_UNIQUE = SCHEMA_PREFIX
      + CONSTRAINT_SQ_INPUT_UNIQUE_NAME_TYPE_CONFIG;

  // SQ_LINK
  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_CONNECTION_NAME = "SQ_CONNECTION";

  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_CONNECTION = SCHEMA_PREFIX + TABLE_SQ_CONNECTION_NAME;

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_ID = "SQN_ID";
  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_NAME = "SQN_NAME";
  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_CONNECTOR = "SQN_CONNECTOR";
  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQ_LNK_CONNECTOR = "SQ_LNK_CONNECTOR";

  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_CREATION_USER = "SQN_CREATION_USER";
  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_CREATION_DATE = "SQN_CREATION_DATE";
  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_UPDATE_USER = "SQN_UPDATE_USER";
  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_UPDATE_DATE = "SQN_UPDATE_DATE";
  @Deprecated
  // used only for upgrade
  public static final String COLUMN_SQN_ENABLED = "SQN_ENABLED";
  // constraints relevant only in derby
  @Deprecated
  public static final String CONSTRAINT_SQN_SQC_NAME = CONSTRAINT_PREFIX + "SQN_SQC";
  public static final String CONSTRAINT_SQ_LNK_SQC_NAME = CONSTRAINT_PREFIX + "SQ_LNK_SQC";

  @Deprecated
  public static final String CONSTRAINT_SQN_SQC = SCHEMA_PREFIX + CONSTRAINT_SQN_SQC_NAME;
  // FK constraint on the connector configurable
  public static final String CONSTRAINT_SQ_LNK_SQC = SCHEMA_PREFIX + CONSTRAINT_SQ_LNK_SQC_NAME;

  public static final String CONSTRAINT_SQ_LNK_NAME_UNIQUE_NAME = CONSTRAINT_PREFIX
      + "SQ_LNK_NAME_UNIQUE";
  public static final String CONSTRAINT_SQ_LNK_NAME_UNIQUE = SCHEMA_PREFIX
      + CONSTRAINT_SQ_LNK_NAME_UNIQUE_NAME;

  // SQ_CONNECTION
  @Deprecated
  public static final String COLUMN_SQB_CONNECTION = "SQB_CONNECTION";

  @Deprecated
  public static final String COLUMN_SQB_TYPE = "SQB_TYPE";

  @Deprecated
  // used only for upgrade since the table CONNECTION changed to LINK
  public static final String COLUMN_SQB_FROM_CONNECTION = "SQB_FROM_CONNECTION";

  @Deprecated
  // used only for upgrade since the table CONNECTION changed to LINK
  public static final String COLUMN_SQB_TO_CONNECTION = "SQB_TO_CONNECTION";

  // constraints relevant only in derby
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQB_SQN_NAME = CONSTRAINT_PREFIX + "SQB_SQN";
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQB_SQN = SCHEMA_PREFIX + CONSTRAINT_SQB_SQN_NAME;
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQB_SQN_FROM_NAME = CONSTRAINT_PREFIX + "SQB_SQN_FROM";
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQB_SQN_FROM = SCHEMA_PREFIX + CONSTRAINT_SQB_SQN_FROM_NAME;
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQB_SQN_TO_NAME = CONSTRAINT_PREFIX + "SQB_SQN_TO";
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQB_SQN_TO = SCHEMA_PREFIX + CONSTRAINT_SQB_SQN_TO_NAME;

  public static final String CONSTRAINT_SQB_SQ_LNK_NAME = CONSTRAINT_PREFIX + "SQB_SQ_LNK";

  public static final String CONSTRAINT_SQB_SQ_LNK = SCHEMA_PREFIX + CONSTRAINT_SQB_SQ_LNK_NAME;

  public static final String CONSTRAINT_SQB_SQ_LNK_FROM_NAME = CONSTRAINT_PREFIX
      + "SQB_SQ_LNK_FROM";

  public static final String CONSTRAINT_SQB_SQ_LNK_FROM = SCHEMA_PREFIX
      + CONSTRAINT_SQB_SQ_LNK_FROM_NAME;

  public static final String CONSTRAINT_SQB_SQ_LNK_TO_NAME = CONSTRAINT_PREFIX + "SQB_SQ_LNK_TO";

  public static final String CONSTRAINT_SQB_SQ_LNK_TO = SCHEMA_PREFIX
      + CONSTRAINT_SQB_SQ_LNK_TO_NAME;

  public static final String CONSTRAINT_SQB_NAME_UNIQUE_NAME = CONSTRAINT_PREFIX
      + "SQB_NAME_UNIQUE";

  // relevant constraints relevant only in derby
  public static final String CONSTRAINT_SQB_NAME_UNIQUE = SCHEMA_PREFIX
      + CONSTRAINT_SQB_NAME_UNIQUE_NAME;

  // SQ_LINK_INPUT
  @Deprecated
  // only used for upgrade
  public static final String TABLE_SQ_CONNECTION_INPUT_NAME = "SQ_CONNECTION_INPUT";

  @Deprecated
  // only used for upgrade
  public static final String TABLE_SQ_CONNECTION_INPUT = SCHEMA_PREFIX
      + TABLE_SQ_CONNECTION_INPUT_NAME;

  @Deprecated
  // only used for upgrade
  public static final String COLUMN_SQNI_CONNECTION = "SQNI_CONNECTION";

  @Deprecated
  // only used for upgrade
  public static final String COLUMN_SQNI_INPUT = "SQNI_INPUT";

  @Deprecated
  // only used for upgrade
  public static final String COLUMN_SQNI_VALUE = "SQNI_VALUE";

  // constraints relevant only in derby
  @Deprecated
  // only used for upgrade
  public static final String CONSTRAINT_SQNI_SQN_NAME = CONSTRAINT_PREFIX + "SQNI_SQN";
  public static final String CONSTRAINT_SQ_LNKI_SQ_LNK_NAME = CONSTRAINT_PREFIX + "SQ_LNKI_SQ_LNK";

  @Deprecated
  // only used for upgrade
  public static final String CONSTRAINT_SQNI_SQN = SCHEMA_PREFIX + CONSTRAINT_SQNI_SQN_NAME;
  public static final String CONSTRAINT_SQ_LNKI_SQ_LNK = SCHEMA_PREFIX
      + CONSTRAINT_SQ_LNKI_SQ_LNK_NAME;

  @Deprecated
  // only used for upgrade
  public static final String CONSTRAINT_SQNI_SQI_NAME = CONSTRAINT_PREFIX + "SQNI_SQI";
  public static final String CONSTRAINT_SQ_LNKI_SQI_NAME = CONSTRAINT_PREFIX + "SQ_LNKI_SQI";
  @Deprecated
  // only used for upgrade
  public static final String CONSTRAINT_SQNI_SQI = SCHEMA_PREFIX + CONSTRAINT_SQNI_SQI_NAME;
  public static final String CONSTRAINT_SQ_LNKI_SQI = SCHEMA_PREFIX + CONSTRAINT_SQ_LNKI_SQI_NAME;

  // SQ_JOB_INPUT
  // constraints relevant only in derby
  public static final String CONSTRAINT_SQBI_SQB_NAME = CONSTRAINT_PREFIX + "SQBI_SQB";

  public static final String CONSTRAINT_SQBI_SQB = SCHEMA_PREFIX + CONSTRAINT_SQBI_SQB_NAME;

  public static final String CONSTRAINT_SQBI_SQI_NAME = CONSTRAINT_PREFIX + "SQBI_SQI";

  public static final String CONSTRAINT_SQBI_SQI = SCHEMA_PREFIX + CONSTRAINT_SQBI_SQI_NAME;

  // SQ_SUBMISSION
  public static final String CONSTRAINT_SQS_SQB_NAME = CONSTRAINT_PREFIX + "SQS_SQB";

  public static final String CONSTRAINT_SQS_SQB = SCHEMA_PREFIX + CONSTRAINT_SQS_SQB_NAME;

  // SQ_COUNTER_SUBMISSION
  // constraints relevant only in derby
  public static final String CONSTRAINT_SQRS_SQG_NAME = CONSTRAINT_PREFIX + "SQRS_SQG";

  public static final String CONSTRAINT_SQRS_SQG = SCHEMA_PREFIX + CONSTRAINT_SQRS_SQG_NAME;

  public static final String CONSTRAINT_SQRS_SQR_NAME = CONSTRAINT_PREFIX + "SQRS_SQR";

  public static final String CONSTRAINT_SQRS_SQR = SCHEMA_PREFIX + CONSTRAINT_SQRS_SQR_NAME;

  public static final String CONSTRAINT_SQRS_SQS_NAME = CONSTRAINT_PREFIX + "SQRS_SQS";

  public static final String CONSTRAINT_SQRS_SQS = SCHEMA_PREFIX + CONSTRAINT_SQRS_SQS_NAME;

  /**
   * List of expected tables for first version; This list here is for backward
   * compatibility.
   */
  public static final Set<String> tablesV1;
  static {
    tablesV1 = new HashSet<String>();
    tablesV1.add(TABLE_SQ_CONNECTOR_NAME);
    tablesV1.add(TABLE_SQ_CONNECTION_NAME);
    tablesV1.add(TABLE_SQ_CONNECTION_INPUT_NAME);
    tablesV1.add(TABLE_SQ_COUNTER_NAME);
    tablesV1.add(TABLE_SQ_COUNTER_GROUP_NAME);
    tablesV1.add(TABLE_SQ_COUNTER_SUBMISSION_NAME);
    tablesV1.add(TABLE_SQ_FORM_NAME);
    tablesV1.add(TABLE_SQ_INPUT_NAME);
    tablesV1.add(TABLE_SQ_JOB_NAME);
    tablesV1.add(TABLE_SQ_JOB_INPUT_NAME);
    tablesV1.add(TABLE_SQ_SUBMISSION_NAME);
  }

  private DerbySchemaConstants() {
    // Disable explicit object creation
  }
}
