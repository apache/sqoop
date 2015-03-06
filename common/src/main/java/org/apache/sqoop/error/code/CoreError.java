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

public enum CoreError implements ErrorCode {

  /** An unknown error has occurred. */
  CORE_0000("An unknown error has occurred"),

  /** The system was unable to find the configuration directory. */
  CORE_0001("Invalid confiugration directory"),

  /** The system was unable to load bootstrap configuration. */
  CORE_0002("Invalid bootstrap configuration"),

  /**
   * The bootstrap configuration did not contain the class name of
   * configuration provider.
   */
  CORE_0003("No configuration provider set for the system"),

  /** The system was unable locate configuration provider implementation.*/
  CORE_0004("Configuration provider was not found"),

  /** The system was unable to load configuration provider */
  CORE_0005("Unable to load configuration provider"),

  /**
   * The PropertiesConfigurationProvider is unable to load the configuration
   * properties file.
   */
  CORE_0006("Properties configuration provider unable to load config file"),

  /** The configuration system has not been initialized correctly. */
  CORE_0007("System not initialized"),

  /** The system has not been reconfigured */
  CORE_0008("System not reconfigured");

  ;

  private final String message;

  private CoreError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
