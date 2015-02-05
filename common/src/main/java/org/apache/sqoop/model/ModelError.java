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
package org.apache.sqoop.model;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.ErrorCode;

/**
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum ModelError implements ErrorCode {

  MODEL_001("Attempt to pass two different set of MConfigs for single job type."),

  MODEL_002("Creating MJob of different job types"),

  MODEL_003("Object is not valid configuration object"),

  MODEL_004("Usage of unsupported data type"),

  MODEL_005("Can't get field value"),

  MODEL_006("Incompatible config list and configuration object"),

  MODEL_007("Primitive types in configuration objects are not allowed"),

  MODEL_008("Invalid input value"),

  MODEL_009("Invalid input name"),

  MODEL_010("Config do not exist"),

  MODEL_011("Input do not exist"),

  MODEL_012("Form name attribute should be unique across a configuration object"),

  MODEL_013("Form name attribute should not contain unsupported characters"),

  MODEL_014("Form name attribute cannot be more than 30 characters long"),

  MODEL_015("Can't get value from object"),

  MODEL_016("Can't instantiate class"),

  MODEL_017("Config Input override name does nto exist"),
  /**
   * Config Input that is set to USER_ONLY editable cannot override other
   * USER_ONLY input
   **/
  MODEL_018("Config Input cannot override USER_ONLY attribute"),

  /** Config Input cannot override itself */
  MODEL_019("Config Input cannot override itself"),

  ;

  private final String message;

  private ModelError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
