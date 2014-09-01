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

  // SQ_FORM

  public static final String TABLE_SQ_FORM_NAME = "SQ_FORM";

  public static final String TABLE_SQ_FORM = SCHEMA_PREFIX
    + TABLE_SQ_FORM_NAME;

  public static final String COLUMN_SQF_ID = "SQF_ID";

  public static final String COLUMN_SQF_CONNECTOR = "SQF_CONNECTOR";

  public static final String COLUMN_SQF_OPERATION = "SQF_OPERATION";

  public static final String COLUMN_SQF_DIRECTION = "SQF_DIRECTION";

  public static final String COLUMN_SQF_NAME = "SQF_NAME";

  public static final String COLUMN_SQF_TYPE = "SQF_TYPE";

  public static final String COLUMN_SQF_INDEX = "SQF_INDEX";

  public static final String CONSTRAINT_SQF_SQC_NAME = CONSTRAINT_PREFIX + "SQF_SQC";

  public static final String CONSTRAINT_SQF_SQC = SCHEMA_PREFIX + CONSTRAINT_SQF_SQC_NAME;

  // SQ_INPUT

  public static final String TABLE_SQ_INPUT_NAME = "SQ_INPUT";

  public static final String TABLE_SQ_INPUT = SCHEMA_PREFIX
    + TABLE_SQ_INPUT_NAME;

  public static final String COLUMN_SQI_ID = "SQI_ID";

  public static final String COLUMN_SQI_NAME = "SQI_NAME";

  public static final String COLUMN_SQI_FORM = "SQI_FORM";

  public static final String COLUMN_SQI_INDEX = "SQI_INDEX";

  public static final String COLUMN_SQI_TYPE = "SQI_TYPE";

  public static final String COLUMN_SQI_STRMASK = "SQI_STRMASK";

  public static final String COLUMN_SQI_STRLENGTH = "SQI_STRLENGTH";

  public static final String COLUMN_SQI_ENUMVALS = "SQI_ENUMVALS";

  public static final String CONSTRAINT_SQI_SQF_NAME = CONSTRAINT_PREFIX + "SQI_SQF";

  public static final String CONSTRAINT_SQI_SQF = SCHEMA_PREFIX + CONSTRAINT_SQI_SQF_NAME;

  // SQ_CONNECTION

  public static final String TABLE_SQ_CONNECTION_NAME = "SQ_CONNECTION";

  public static final String TABLE_SQ_CONNECTION = SCHEMA_PREFIX
    + TABLE_SQ_CONNECTION_NAME;

  public static final String COLUMN_SQN_ID = "SQN_ID";

  public static final String COLUMN_SQN_NAME = "SQN_NAME";

  public static final String COLUMN_SQN_CONNECTOR = "SQN_CONNECTOR";

  public static final String COLUMN_SQN_CREATION_USER = "SQN_CREATION_USER";

  public static final String COLUMN_SQN_CREATION_DATE = "SQN_CREATION_DATE";

  public static final String COLUMN_SQN_UPDATE_USER = "SQN_UPDATE_USER";

  public static final String COLUMN_SQN_UPDATE_DATE = "SQN_UPDATE_DATE";

  public static final String COLUMN_SQN_ENABLED = "SQN_ENABLED";

  public static final String CONSTRAINT_SQN_SQC_NAME = CONSTRAINT_PREFIX + "SQN_SQC";

  public static final String CONSTRAINT_SQN_SQC = SCHEMA_PREFIX + CONSTRAINT_SQN_SQC_NAME;

  // SQ_JOB

  public static final String TABLE_SQ_JOB_NAME = "SQ_JOB";

  public static final String TABLE_SQ_JOB = SCHEMA_PREFIX
    + TABLE_SQ_JOB_NAME;

  public static final String COLUMN_SQB_ID = "SQB_ID";

  public static final String COLUMN_SQB_NAME = "SQB_NAME";

  public static final String COLUMN_SQB_CONNECTION = "SQB_CONNECTION";

  public static final String COLUMN_SQB_TYPE = "SQB_TYPE";

  public static final String COLUMN_SQB_FROM_CONNECTION = "SQB_FROM_CONNECTION";

  public static final String COLUMN_SQB_TO_CONNECTION = "SQB_TO_CONNECTION";

  public static final String COLUMN_SQB_CREATION_USER = "SQB_CREATION_USER";

  public static final String COLUMN_SQB_CREATION_DATE = "SQB_CREATION_DATE";

  public static final String COLUMN_SQB_UPDATE_USER = "SQB_UPDATE_USER";

  public static final String COLUMN_SQB_UPDATE_DATE = "SQB_UPDATE_DATE";

  public static final String COLUMN_SQB_ENABLED = "SQB_ENABLED";

  public static final String CONSTRAINT_SQB_SQN_NAME = CONSTRAINT_PREFIX + "SQB_SQN";

  public static final String CONSTRAINT_SQB_SQN = SCHEMA_PREFIX + CONSTRAINT_SQB_SQN_NAME;

  public static final String CONSTRAINT_SQB_SQN_FROM_NAME = CONSTRAINT_PREFIX + "SQB_SQN_FROM";

  public static final String CONSTRAINT_SQB_SQN_FROM = SCHEMA_PREFIX + CONSTRAINT_SQB_SQN_FROM_NAME;

  public static final String CONSTRAINT_SQB_SQN_TO_NAME = CONSTRAINT_PREFIX + "SQB_SQN_TO";

  public static final String CONSTRAINT_SQB_SQN_TO = SCHEMA_PREFIX + CONSTRAINT_SQB_SQN_TO_NAME;

  // SQ_CONNECTION_INPUT

  public static final String TABLE_SQ_CONNECTION_INPUT_NAME =
    "SQ_CONNECTION_INPUT";

  public static final String TABLE_SQ_CONNECTION_INPUT = SCHEMA_PREFIX
    + TABLE_SQ_CONNECTION_INPUT_NAME;

  public static final String COLUMN_SQNI_CONNECTION = "SQNI_CONNECTION";

  public static final String COLUMN_SQNI_INPUT = "SQNI_INPUT";

  public static final String COLUMN_SQNI_VALUE = "SQNI_VALUE";

  public static final String CONSTRAINT_SQNI_SQN_NAME = CONSTRAINT_PREFIX + "SQNI_SQN";

  public static final String CONSTRAINT_SQNI_SQN = SCHEMA_PREFIX + CONSTRAINT_SQNI_SQN_NAME;

  public static final String CONSTRAINT_SQNI_SQI_NAME = CONSTRAINT_PREFIX + "SQNI_SQI";

  public static final String CONSTRAINT_SQNI_SQI = SCHEMA_PREFIX + CONSTRAINT_SQNI_SQI_NAME;

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
