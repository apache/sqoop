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

public enum HdfsConnectorError implements ErrorCode{
  /** Error occurs during partitioner run */
  GENERIC_HDFS_CONNECTOR_0000("Error occurs during partitioner run"),
  /** Error occurs during extractor run */
  GENERIC_HDFS_CONNECTOR_0001("Error occurs during extractor run"),
  /** Unsupported output format type found **/
  GENERIC_HDFS_CONNECTOR_0002("Unknown output format type"),
  /** The system was unable to load the specified class. */
  GENERIC_HDFS_CONNECTOR_0003("Unable to load the specified class"),
  /** The system was unable to instantiate the specified class. */
  GENERIC_HDFS_CONNECTOR_0004("Unable to instantiate the specified class"),
  /** Error occurs during loader run */
  GENERIC_HDFS_CONNECTOR_0005("Error occurs during loader run"),
  GENERIC_HDFS_CONNECTOR_0006("Unknown job type"),

  GENERIC_HDFS_CONNECTOR_0007("Invalid output directory"),

  GENERIC_HDFS_CONNECTOR_0008("Error occurs during destroyer run"),

  ;

  private final String message;

  private HdfsConnectorError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }

}
