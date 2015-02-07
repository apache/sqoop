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

  // SQ_CONNECTOR
  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_CONNECTOR_NAME = "SQ_CONNECTOR";

  // SQ_CONFIG
  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_FORM_NAME = "SQ_FORM";

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
  public static final String COLUMN_SQI_FORM = "SQI_FORM";

  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQI_SQF_NAME = CONSTRAINT_PREFIX + "SQI_SQF";

  // SQ_LINK
  @Deprecated
  // used only for upgrade
  public static final String TABLE_SQ_CONNECTION_NAME = "SQ_CONNECTION";

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
  public static final String CONSTRAINT_SQB_SQN_FROM_NAME = CONSTRAINT_PREFIX + "SQB_SQN_FROM";
  @Deprecated
  // used only for upgrade
  public static final String CONSTRAINT_SQB_SQN_TO_NAME = CONSTRAINT_PREFIX + "SQB_SQN_TO";

  // SQ_LINK_INPUT
  @Deprecated
  // only used for upgrade
  public static final String TABLE_SQ_CONNECTION_INPUT_NAME = "SQ_CONNECTION_INPUT";

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

  @Deprecated
  // only used for upgrade
  public static final String CONSTRAINT_SQNI_SQI_NAME = CONSTRAINT_PREFIX + "SQNI_SQI";

  @Deprecated
 // used only for upgrade
  public static final String COLUMN_SQS_EXCEPTION = "SQS_EXCEPTION";
  @Deprecated
 // used only for upgrade
  public static final String COLUMN_SQS_EXCEPTION_TRACE = "SQS_EXCEPTION_TRACE";

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
