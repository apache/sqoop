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

public enum GenericJdbcConnectorError implements ErrorCode {

  /** Unable to load the driver class. */
  GENERIC_JDBC_CONNECTOR_0000("Unable to load the driver class"),

  /** Unable to get a connection. */
  GENERIC_JDBC_CONNECTOR_0001("Unable to get a connection"),

  /** Unable to execute the SQL statement. */
  GENERIC_JDBC_CONNECTOR_0002("Unable to execute the SQL statement"),

  /** Unable to access meta data. */
  GENERIC_JDBC_CONNECTOR_0003("Unable to access meta data"),

  /** Error occurs while retrieving data from result. */
  GENERIC_JDBC_CONNECTOR_0004("Error occurs while retrieving data from result"),

  /** No column is found to partition data. */
  GENERIC_JDBC_CONNECTOR_0005("No column is found to partition data"),

  /** No boundaries are found for partition column. */
  GENERIC_JDBC_CONNECTOR_0006("No boundaries are found for partition column"),

  /** The table name and the table sql cannot be specify together. */
  GENERIC_JDBC_CONNECTOR_0007("The table name and the table sql "
      + "cannot be specified together"),

  /** Neither the table name nor the table sql are specified. */
  GENERIC_JDBC_CONNECTOR_0008("Neither the table name nor the table sql "
      + "are specified"),

  /** No substitute token in the specified sql. */
  GENERIC_JDBC_CONNECTOR_0010("No substitute token in the specified sql"),

  /** The type is not supported. */
  GENERIC_JDBC_CONNECTOR_0011("The type is not supported"),

  /** The required option has not been set yet. */
  GENERIC_JDBC_CONNECTOR_0012("The required option has not been set yet"),

  /** No parameter marker in the specified sql. */
  GENERIC_JDBC_CONNECTOR_0013("No parameter marker in the specified sql"),

  /** The table columns cannot be specified when
   *  the table sql is specified during export. */
  GENERIC_JDBC_CONNECTOR_0014("The table columns cannot be specified "
      + "when the table sql is specified during export"),

  /** Unsupported values in partition column */
  GENERIC_JDBC_CONNECTOR_0015("Partition column contains unsupported values"),

  /** Can't fetch schema */
  GENERIC_JDBC_CONNECTOR_0016("Can't fetch schema"),

  /** Neither the table name nor the table sql are specified. */
  GENERIC_JDBC_CONNECTOR_0017("The stage table is not empty."),

  GENERIC_JDBC_CONNECTOR_0018("Error occurred while transferring data from " +
    "stage table to destination table."),

  GENERIC_JDBC_CONNECTOR_0019("Table name extraction not supported."),

  GENERIC_JDBC_CONNECTOR_0020("Unknown direction."),

  GENERIC_JDBC_CONNECTOR_0021("Schema column size do not match the result set column size"),

  GENERIC_JDBC_CONNECTOR_0022("Can't find maximal value of column"),

  GENERIC_JDBC_CONNECTOR_0023("Received error from the database"),

  ;

  private final String message;

  private GenericJdbcConnectorError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
