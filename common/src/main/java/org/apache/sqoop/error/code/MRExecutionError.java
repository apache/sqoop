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
package org.apache.sqoop.error.code;

import org.apache.sqoop.common.ErrorCode;

/**
 *
 */
public enum MRExecutionError implements ErrorCode {

  MAPRED_EXEC_0000("Unknown error"),

  /** Error occurs during job execution. */
  MAPRED_EXEC_0008("Error occurs during job execution"),

  /** The system was unable to load the specified class. */
  MAPRED_EXEC_0009("Unable to load the specified class"),

  /** The system was unable to instantiate the specified class. */
  MAPRED_EXEC_0010("Unable to instantiate the specified class"),

  /** The parameter already exists in the context */
  MAPRED_EXEC_0011("The parameter already exists in the context"),

  /** The type is not supported */
  MAPRED_EXEC_0012("The type is not supported"),

  /** Cannot write to the data writer */
  MAPRED_EXEC_0013("Cannot write to the data writer"),

  /** Cannot read from the data reader */
  MAPRED_EXEC_0014("Cannot read to the data reader"),

  /** Unable to write data due to interrupt */
  MAPRED_EXEC_0015("Unable to write data due to interrupt"),

  /** Unable to read data due to interrupt */
  MAPRED_EXEC_0016("Unable to read data due to interrupt"),

  /** Error occurs during extractor run */
  MAPRED_EXEC_0017("Error occurs during extractor run"),

  /** Error occurs during loader run */
  MAPRED_EXEC_0018("Error occurs during loader run"),

  MAPRED_EXEC_0019("Data have not been completely consumed yet"),

  /** The required option has not been set yet */
  MAPRED_EXEC_0020("The required option has not been set yet"),

  /** Error occurs during partitioner run */
  MAPRED_EXEC_0021("Error occurs during partitioner run"),

  /** Unable to parse because it is not properly delimited */
  MAPRED_EXEC_0022("Unable to parse because it is not properly delimited"),

  /** Unknown job type */
  MAPRED_EXEC_0023("Unknown job type"),

  /** Unsupported output format type found **/
  MAPRED_EXEC_0024("Unknown output format type"),

  /** Got invalid number of partitions from Partitioner */
  MAPRED_EXEC_0025("Retrieved invalid number of partitions from Partitioner"),

  ;

  private final String message;

  private MRExecutionError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
