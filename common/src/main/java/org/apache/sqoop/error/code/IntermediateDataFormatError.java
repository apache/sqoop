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
package org.apache.sqoop.error.code;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.ErrorCode;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum IntermediateDataFormatError implements ErrorCode {
  /** An unknown error has occurred. */
  INTERMEDIATE_DATA_FORMAT_0000("An unknown error has occurred."),

  /** Number of columns in schema does not match the data set. */
  INTERMEDIATE_DATA_FORMAT_0001("Wrong number of columns."),

  /** Schema is missing in the IDF. */
  INTERMEDIATE_DATA_FORMAT_0002("Schema is null."),

  INTERMEDIATE_DATA_FORMAT_0003("JSON parse error"),

  /** Column type isn't known by Intermediate Data Format. */
  INTERMEDIATE_DATA_FORMAT_0004("Unknown column type."),

  /** Column value cannot be null. */
  INTERMEDIATE_DATA_FORMAT_0005("Column value cannot be null"),

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
