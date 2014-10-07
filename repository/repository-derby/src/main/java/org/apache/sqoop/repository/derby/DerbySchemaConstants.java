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

import java.util.HashSet;
import java.util.Set;

public final class DerbySchemaConstants {

  public static final String SCHEMA_SQOOP = "SQOOP";

  private static final String SCHEMA_PREFIX = SCHEMA_SQOOP + ".";

  private static final String CONSTRAINT_PREFIX = "FK_";

  // SQ_SYSTEM

  public static final String TABLE_SQ_SYSTEM_NAME = "SQ_SYSTEM";

  public static final String TABLE_SQ_SYSTEM = SCHEMA_PREFIX
    + TABLE_SQ_SYSTEM_NAME;

  public static final String COLUMN_SQM_ID = "SQM_ID";

  public static final String COLUMN_SQM_KEY = "SQM_KEY";

  public static final String COLUMN_SQM_VALUE = "SQM_VALUE";

  // SQ_CONNECTOR

  public static final String TABLE_SQ_CONNECTOR_NAME = "SQ_CONNECTOR";

  public static final String TABLE_SQ_CONNECTOR = SCHEMA_PREFIX
    + TABLE_SQ_CONNECTOR_NAME;

  public static final String COLUMN_SQC_ID = "SQC_ID";

  public static final String COLUMN_SQC_NAME = "SQC_NAME";

  public static final String COLUMN_SQC_CLASS = "SQC_CLASS";

  public static final String COLUMN_SQC_VERSION = "SQC_VERSION";

  // SQ_CONFIG

  public static final String TABLE_SQ_CONFIG_NAME = "SQ_CONFIG";

  public static final String TABLE_SQ_CONFIG = SCHEMA_PREFIX
    + TABLE_SQ_CONFIG_NAME;

  public static final String COLUMN_SQ_CFG_ID = "SQ_CFG_ID";

  public static final String COLUMN_SQ_CFG_OWNER = "SQ_CFG_OWNER";

  public static final String COLUMN_SQ_CFG_OPERATION = "SQ_CFG_OPERATION";

  public static final String COLUMN_SQ_CFG_DIRECTION = "SQ_CFG_DIRECTION";

  public static final String COLUMN_SQ_CFG_NAME = "SQ_CFG_NAME";

  public static final String COLUMN_SQ_CFG_TYPE = "SQ_CFG_TYPE";

  public static final String COLUMN_SQ_CFG_INDEX = "SQ_CFG_INDEX";

  public static final String CONSTRAINT_SQ_CFG_SQC_NAME = CONSTRAINT_PREFIX + "SQ_CFG_SQC";

  public static final String CONSTRAINT_SQ_CFG_SQC = SCHEMA_PREFIX + CONSTRAINT_SQ_CFG_SQC_NAME;

  // SQ_INPUT

  public static final String TABLE_SQ_INPUT_NAME = "SQ_INPUT";

  public static final String TABLE_SQ_INPUT = SCHEMA_PREFIX
    + TABLE_SQ_INPUT_NAME;

  public static final String COLUMN_SQI_ID = "SQI_ID";

  public static final String COLUMN_SQI_NAME = "SQI_NAME";

  public static final String COLUMN_SQI_CONFIG = "SQI_CONFIG";

  public static final String COLUMN_SQI_INDEX = "SQI_INDEX";

  public static final String COLUMN_SQI_TYPE = "SQI_TYPE";

  public static final String COLUMN_SQI_STRMASK = "SQI_STRMASK";

  public static final String COLUMN_SQI_STRLENGTH = "SQI_STRLENGTH";

  public static final String COLUMN_SQI_ENUMVALS = "SQI_ENUMVALS";

  public static final String CONSTRAINT_SQI_SQ_CFG_NAME = CONSTRAINT_PREFIX + "SQI_SQ_CFG";

  public static final String CONSTRAINT_SQI_SQ_CFG = SCHEMA_PREFIX + CONSTRAINT_SQI_SQ_CFG_NAME;

  public static final String TABLE_SQ_LINK_NAME = "SQ_LINK";

  public static final String TABLE_SQ_LINK = SCHEMA_PREFIX
      + TABLE_SQ_LINK_NAME;

  public static final String COLUMN_SQ_LNK_ID = "SQ_LNK_ID";

  public static final String COLUMN_SQ_LNK_NAME = "SQ_LNK_NAME";

  public static final String COLUMN_SQ_LNK_CONNECTOR = "SQ_LNK_CONNECTOR";

  public static final String COLUMN_SQ_LNK_CREATION_USER = "SQ_LNK_CREATION_USER";

  public static final String COLUMN_SQ_LNK_CREATION_DATE = "SQ_LNK_CREATION_DATE";

  public static final String COLUMN_SQ_LNK_UPDATE_USER = "SQ_LNK_UPDATE_USER";

  public static final String COLUMN_SQ_LNK_UPDATE_DATE = "SQ_LNK_UPDATE_DATE";

  public static final String COLUMN_SQ_LNK_ENABLED = "SQ_LNK_ENABLED";

  public static final String CONSTRAINT_SQ_LNK_SQC_NAME = CONSTRAINT_PREFIX + "SQ_LNK_SQC";

  public static final String CONSTRAINT_SQ_LNK_SQC = SCHEMA_PREFIX + CONSTRAINT_SQ_LNK_SQC_NAME;

  public static final String CONSTRAINT_SQ_LNK_NAME_UNIQUE_NAME = CONSTRAINT_PREFIX + "SQ_LNK_NAME_UNIQUE";

  public static final String CONSTRAINT_SQ_LNK_NAME_UNIQUE = SCHEMA_PREFIX + CONSTRAINT_SQ_LNK_NAME_UNIQUE_NAME;

  // SQ_JOB

  public static final String TABLE_SQ_JOB_NAME = "SQ_JOB";

  public static final String TABLE_SQ_JOB = SCHEMA_PREFIX
    + TABLE_SQ_JOB_NAME;

  public static final String COLUMN_SQB_ID = "SQB_ID";

  public static final String COLUMN_SQB_NAME = "SQB_NAME";

  public static final String COLUMN_SQB_LINK = "SQB_LINK";

  public static final String COLUMN_SQB_TYPE = "SQB_TYPE";

  public static final String COLUMN_SQB_FROM_LINK = "SQB_FROM_LINK";

  public static final String COLUMN_SQB_TO_LINK = "SQB_TO_LINK";

  public static final String COLUMN_SQB_CREATION_USER = "SQB_CREATION_USER";

  public static final String COLUMN_SQB_CREATION_DATE = "SQB_CREATION_DATE";

  public static final String COLUMN_SQB_UPDATE_USER = "SQB_UPDATE_USER";

  public static final String COLUMN_SQB_UPDATE_DATE = "SQB_UPDATE_DATE";

  public static final String COLUMN_SQB_ENABLED = "SQB_ENABLED";

  public static final String CONSTRAINT_SQB_SQ_LNK_NAME = CONSTRAINT_PREFIX + "SQB_SQ_LNK";

  public static final String CONSTRAINT_SQB_SQ_LNK = SCHEMA_PREFIX + CONSTRAINT_SQB_SQ_LNK_NAME;

  public static final String CONSTRAINT_SQB_SQ_LNK_FROM_NAME = CONSTRAINT_PREFIX + "SQB_SQ_LNK_FROM";

  public static final String CONSTRAINT_SQB_SQ_LNK_FROM = SCHEMA_PREFIX + CONSTRAINT_SQB_SQ_LNK_FROM_NAME;

  public static final String CONSTRAINT_SQB_SQ_LNK_TO_NAME = CONSTRAINT_PREFIX + "SQB_SQ_LNK_TO";

  public static final String CONSTRAINT_SQB_SQ_LNK_TO = SCHEMA_PREFIX + CONSTRAINT_SQB_SQ_LNK_TO_NAME;

  public static final String CONSTRAINT_SQB_NAME_UNIQUE_NAME = CONSTRAINT_PREFIX + "SQB_NAME_UNIQUE";

  public static final String CONSTRAINT_SQB_NAME_UNIQUE = SCHEMA_PREFIX + CONSTRAINT_SQB_NAME_UNIQUE_NAME;

  // SQ_CONNECTION_INPUT

  public static final String TABLE_SQ_LINK_INPUT_NAME =
    "SQ_LINK_INPUT";

  public static final String TABLE_SQ_LINK_INPUT = SCHEMA_PREFIX
    + TABLE_SQ_LINK_INPUT_NAME;

  public static final String COLUMN_SQ_LNKI_LINK = "SQ_LNKI_LINK";

  public static final String COLUMN_SQ_LNKI_INPUT = "SQ_LNKI_INPUT";

  public static final String COLUMN_SQ_LNKI_VALUE = "SQ_LNKI_VALUE";

  public static final String CONSTRAINT_SQ_LNKI_SQ_LNK_NAME = CONSTRAINT_PREFIX + "SQ_LNKI_SQ_LNK";

  public static final String CONSTRAINT_SQ_LNKI_SQ_LNK = SCHEMA_PREFIX + CONSTRAINT_SQ_LNKI_SQ_LNK_NAME;

  public static final String CONSTRAINT_SQ_LNKI_SQI_NAME = CONSTRAINT_PREFIX + "SQ_LNKI_SQI";

  public static final String CONSTRAINT_SQ_LNKI_SQI = SCHEMA_PREFIX + CONSTRAINT_SQ_LNKI_SQI_NAME;

  // SQ_JOB_INPUT

  public static final String TABLE_SQ_JOB_INPUT_NAME =
    "SQ_JOB_INPUT";

  public static final String TABLE_SQ_JOB_INPUT = SCHEMA_PREFIX
      + TABLE_SQ_JOB_INPUT_NAME;

  public static final String COLUMN_SQBI_JOB = "SQBI_JOB";

  public static final String COLUMN_SQBI_INPUT = "SQBI_INPUT";

  public static final String COLUMN_SQBI_VALUE = "SQBI_VALUE";

  public static final String CONSTRAINT_SQBI_SQB_NAME = CONSTRAINT_PREFIX + "SQBI_SQB";

  public static final String CONSTRAINT_SQBI_SQB = SCHEMA_PREFIX + CONSTRAINT_SQBI_SQB_NAME;

  public static final String CONSTRAINT_SQBI_SQI_NAME = CONSTRAINT_PREFIX + "SQBI_SQI";

  public static final String CONSTRAINT_SQBI_SQI = SCHEMA_PREFIX + CONSTRAINT_SQBI_SQI_NAME;

  // SQ_SUBMISSION

  public static final String TABLE_SQ_SUBMISSION_NAME =
    "SQ_SUBMISSION";

  public static final String TABLE_SQ_SUBMISSION = SCHEMA_PREFIX
    + TABLE_SQ_SUBMISSION_NAME;

  public static final String COLUMN_SQS_ID = "SQS_ID";

  public static final String COLUMN_SQS_JOB = "SQS_JOB";

  public static final String COLUMN_SQS_STATUS = "SQS_STATUS";

  public static final String COLUMN_SQS_CREATION_USER = "SQS_CREATION_USER";

  public static final String COLUMN_SQS_CREATION_DATE = "SQS_CREATION_DATE";

  public static final String COLUMN_SQS_UPDATE_USER = "SQS_UPDATE_USER";

  public static final String COLUMN_SQS_UPDATE_DATE = "SQS_UPDATE_DATE";

  public static final String COLUMN_SQS_EXTERNAL_ID = "SQS_EXTERNAL_ID";

  public static final String COLUMN_SQS_EXTERNAL_LINK = "SQS_EXTERNAL_LINK";

  public static final String COLUMN_SQS_EXCEPTION = "SQS_EXCEPTION";

  public static final String COLUMN_SQS_EXCEPTION_TRACE = "SQS_EXCEPTION_TRACE";

  public static final String CONSTRAINT_SQS_SQB_NAME = CONSTRAINT_PREFIX + "SQS_SQB";

  public static final String CONSTRAINT_SQS_SQB = SCHEMA_PREFIX + CONSTRAINT_SQS_SQB_NAME;

  // SQ_COUNTER_GROUP

  public static final String TABLE_SQ_COUNTER_GROUP_NAME =
    "SQ_COUNTER_GROUP";

  public static final String TABLE_SQ_COUNTER_GROUP = SCHEMA_PREFIX
      + TABLE_SQ_COUNTER_GROUP_NAME;

  public static final String COLUMN_SQG_ID = "SQG_ID";

  public static final String COLUMN_SQG_NAME = "SQG_NAME";

  // SQ_COUNTER_GROUP

  public static final String TABLE_SQ_COUNTER_NAME =
    "SQ_COUNTER";

  public static final String TABLE_SQ_COUNTER = SCHEMA_PREFIX
      + TABLE_SQ_COUNTER_NAME;

  public static final String COLUMN_SQR_ID = "SQR_ID";

  public static final String COLUMN_SQR_NAME = "SQR_NAME";

  // SQ_COUNTER_SUBMISSION

  public static final String TABLE_SQ_COUNTER_SUBMISSION_NAME =
    "SQ_COUNTER_SUBMISSION";

  public static final String TABLE_SQ_COUNTER_SUBMISSION = SCHEMA_PREFIX
    + TABLE_SQ_COUNTER_SUBMISSION_NAME;

  public static final String COLUMN_SQRS_GROUP = "SQRS_GROUP";

  public static final String COLUMN_SQRS_COUNTER = "SQRS_COUNTER";

  public static final String COLUMN_SQRS_SUBMISSION = "SQRS_SUBMISSION";

  public static final String COLUMN_SQRS_VALUE = "SQRS_VALUE";

  public static final String CONSTRAINT_SQRS_SQG_NAME = CONSTRAINT_PREFIX + "SQRS_SQG";

  public static final String CONSTRAINT_SQRS_SQG = SCHEMA_PREFIX + CONSTRAINT_SQRS_SQG_NAME;

  public static final String CONSTRAINT_SQRS_SQR_NAME = CONSTRAINT_PREFIX + "SQRS_SQR";

  public static final String CONSTRAINT_SQRS_SQR = SCHEMA_PREFIX + CONSTRAINT_SQRS_SQR_NAME;

  public static final String CONSTRAINT_SQRS_SQS_NAME = CONSTRAINT_PREFIX + "SQRS_SQS";

  public static final String CONSTRAINT_SQRS_SQS = SCHEMA_PREFIX + CONSTRAINT_SQRS_SQS_NAME;

  /**
   * List of expected tables for first version;
   * This list here is for backward compatibility.
   */
  public static final Set<String> tablesV1;
  static {
    tablesV1 = new HashSet<String>();
    tablesV1.add(TABLE_SQ_CONNECTOR_NAME);
    tablesV1.add(TABLE_SQ_LINK_NAME);
    tablesV1.add(TABLE_SQ_LINK_INPUT_NAME);
    tablesV1.add(TABLE_SQ_COUNTER_NAME);
    tablesV1.add(TABLE_SQ_COUNTER_GROUP_NAME);
    tablesV1.add(TABLE_SQ_COUNTER_SUBMISSION_NAME);
    tablesV1.add(TABLE_SQ_CONFIG_NAME);
    tablesV1.add(TABLE_SQ_INPUT_NAME);
    tablesV1.add(TABLE_SQ_JOB_NAME);
    tablesV1.add(TABLE_SQ_JOB_INPUT_NAME);
    tablesV1.add(TABLE_SQ_SUBMISSION_NAME);
  }

  private DerbySchemaConstants() {
    // Disable explicit object creation
  }
}
