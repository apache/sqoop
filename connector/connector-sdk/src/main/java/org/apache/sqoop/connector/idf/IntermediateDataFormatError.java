/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.connector.idf;

import org.apache.sqoop.common.ErrorCode;

public enum IntermediateDataFormatError implements ErrorCode {
  /** An unknown error has occurred. */
  INTERMEDIATE_DATA_FORMAT_0000("An unknown error has occurred."),

  /** An encoding is missing in the Java native libraries. */
  INTERMEDIATE_DATA_FORMAT_0001("Native character set error."),

  /** Error while escaping a row. */
  INTERMEDIATE_DATA_FORMAT_0002("An error has occurred while escaping a row."),

  /** Error while escaping a row. */
  INTERMEDIATE_DATA_FORMAT_0003("An error has occurred while unescaping a row."),

  /** Column type isn't known by Intermediate Data Format. */
  INTERMEDIATE_DATA_FORMAT_0004("Unknown column type."),

  /** Number of columns in schema does not match the data set. */
  INTERMEDIATE_DATA_FORMAT_0005("Wrong number of columns."),

  /** Schema is missing in the IDF. */
  INTERMEDIATE_DATA_FORMAT_0006("Schema missing."),

  /** For arrays and maps we use JSON representation and incorrect representation results in parse exception*/
  INTERMEDIATE_DATA_FORMAT_0008("JSON parse internal error."),

  /** Unsupported bit values */
  INTERMEDIATE_DATA_FORMAT_0009("Unsupported bit value."),

  ;

  private final String message;

  private IntermediateDataFormatError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
