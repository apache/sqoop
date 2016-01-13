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

public enum ConnectorError implements ErrorCode {

  /** An unknown error has occurred. */
  CONN_0000("An unknown error has occurred"),

  /** The system was not able to initialize the configured connectors. */
  CONN_0001("Unable to initialize connectors"),

  /** No connectors were found in the system. */
  CONN_0002("No connectors were found in the system"),

  /** A problem was encountered while loading the connector configuration. */
  CONN_0003("Failed to load connector configuration"),

  /** A connector configuration file did not include the provider class name.*/
  CONN_0004("Connector configuration did not include provider class name"),

  /** An exception occurred while attempting to instantiate the connector. */
  CONN_0005("Failed to instantiate connector class"),

  /** More than one connectors use the same name resulting in conflict. */
  CONN_0006("More than one connector uses the same name"),

  /** The registration of connector during system initialization failed.*/
  CONN_0007("Connector registration failed"),

  /** The configuration of connector does not specify it's unique name. */
  CONN_0008("No name specified for connector"),

  /**
   * A connector is being registered with the same name as what has been
   * previously registered. Or the connector being registered is the same but
   * it's metadata has changed in an incompatible manner since the last time it
   * was registered.
   */
  CONN_0009("Attempt to register connector with a name associated with a "
      + "previously registered connector; or the connector metadata has "
      + "changed since it was registered previously."),

  /** A connector is not assigned with a valid id yet. */
  CONN_0010("A connector is not assigned with a valid id yet");

  private final String message;

  private ConnectorError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
