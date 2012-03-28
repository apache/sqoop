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

  public static final String TABLE_SQ_CONNECTOR_NAME = "SQ_CONNECTOR";

  public static final String TABLE_SQ_CONNECTOR = SCHEMA_PREFIX
      + TABLE_SQ_CONNECTOR_NAME;

  public static final String COLUMN_SQC_ID = "SQC_ID";

  public static final String COLUMN_SQC_NAME = "SQC_NAME";

  public static final String COLUMN_SQC_CLASS = "SQC_CLASS";

  public static final String TABLE_SQ_FORM_NAME = "SQ_FORM";

  public static final String TABLE_SQ_FORM = SCHEMA_PREFIX
      + TABLE_SQ_FORM_NAME;

  public static final String  COLUMN_SQF_ID = "SQF_ID";

  public static final String COLUMN_SQF_CONNECTOR = "SQF_CONNECTOR";

  public static final String COLUMN_SQF_NAME = "SQF_NAME";

  public static final String COLUMN_SQF_TYPE = "SQF_TYPE";

  public static final String COLUMN_SQF_INDEX = "SQF_INDEX";


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


  public static final String TABLE_SQ_STRING_NAME = "SQ_STRING";
  public static final String TABLE_SQ_STRING = SCHEMA_PREFIX
      + TABLE_SQ_STRING_NAME;

  public static final String COLUMN_SQS_ID = "SQS_ID";

  public static final String COLUMN_SQS_MASK = "SQS_MASK";

  public static final String COLUMN_SQS_LENGTH = "SQS_LENGTH";


  private DerbySchemaConstants() {
    // Disable explicit object creation
  }
}
