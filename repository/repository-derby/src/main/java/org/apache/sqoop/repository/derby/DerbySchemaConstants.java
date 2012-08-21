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

public final class DerbySchemaConstants {

  public static final String SCHEMA_SQOOP = "SQOOP";

  private static final String SCHEMA_PREFIX = SCHEMA_SQOOP + ".";

  // SQ_CONNECTOR

  public static final String TABLE_SQ_CONNECTOR_NAME = "SQ_CONNECTOR";

  public static final String TABLE_SQ_CONNECTOR = SCHEMA_PREFIX
      + TABLE_SQ_CONNECTOR_NAME;

  public static final String COLUMN_SQC_ID = "SQC_ID";

  public static final String COLUMN_SQC_NAME = "SQC_NAME";

  public static final String COLUMN_SQC_CLASS = "SQC_CLASS";

  // SQ_FORM

  public static final String TABLE_SQ_FORM_NAME = "SQ_FORM";

  public static final String TABLE_SQ_FORM = SCHEMA_PREFIX
      + TABLE_SQ_FORM_NAME;

  public static final String COLUMN_SQF_ID = "SQF_ID";

  public static final String COLUMN_SQF_CONNECTOR = "SQF_CONNECTOR";

  public static final String COLUMN_SQF_OPERATION = "SQF_OPERATION";

  public static final String COLUMN_SQF_NAME = "SQF_NAME";

  public static final String COLUMN_SQF_TYPE = "SQF_TYPE";

  public static final String COLUMN_SQF_INDEX = "SQF_INDEX";

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

  // SQ_CONNECTION

  public static final String TABLE_SQ_CONNECTION_NAME = "SQ_CONNECTION";

  public static final String TABLE_SQ_CONNECTION = SCHEMA_PREFIX
      + TABLE_SQ_CONNECTION_NAME;

  public static final String COLUMN_SQN_ID = "SQN_ID";

  public static final String COLUMN_SQN_NAME = "SQN_NAME";

  public static final String COLUMN_SQN_CONNECTOR = "SQN_CONNECTOR";

  // SQ_JOB

  public static final String TABLE_SQ_JOB_NAME = "SQ_JOB";

  public static final String TABLE_SQ_JOB = SCHEMA_PREFIX
      + TABLE_SQ_JOB_NAME;

  public static final String COLUMN_SQB_ID = "SQB_ID";

  public static final String COLUMN_SQB_NAME = "SQB_NAME";

  public static final String COLUMN_SQB_CONNECTION = "SQB_CONNECTION";

  // SQ_CONNECTION_INPUT

  public static final String TABLE_SQ_CONNECTION_INPUT_NAME =
      "SQ_CONNECTION_INPUT";

  public static final String TABLE_SQ_CONNECTION_INPUT = SCHEMA_PREFIX
      + TABLE_SQ_CONNECTION_INPUT_NAME;

  public static final String COLUMN_SQNI_CONNECTION = "SQNI_CONNECTION";

  public static final String COLUMN_SQNI_INPUT = "SQNI_INPUT";

  public static final String COLUMN_SQNI_VALUE = "SQNI_VALUE";

  // SQ_JOB_INPUT

  public static final String TABLE_SQ_JOB_INPUT_NAME =
      "SQ_JOB_INPUT";

  public static final String TABLE_SQ_JOB_INPUT = SCHEMA_PREFIX
      + TABLE_SQ_JOB_INPUT_NAME;

  public static final String COLUMN_SQBI_JOB = "SQBI_JOB";

  public static final String COLUMN_SQBI_INPUT = "SQBI_INPUT";

  public static final String COLUMN_SQBI_VALUE = "SQBI_VALUE";

  private DerbySchemaConstants() {
    // Disable explicit object creation
  }
}
