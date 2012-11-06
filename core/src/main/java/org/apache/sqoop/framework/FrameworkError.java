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
package org.apache.sqoop.framework;

import org.apache.sqoop.common.ErrorCode;

/**
 *
 */
public enum FrameworkError implements ErrorCode {

  FRAMEWORK_0000("Metadata are not registered in repository"),

  FRAMEWORK_0001("Invalid submission engine"),

  FRAMEWORK_0002("Given job is already running"),

  FRAMEWORK_0003("Given job is not running"),

  FRAMEWORK_0004("Unknown job id"),

  FRAMEWORK_0005("Unsupported job type"),

  FRAMEWORK_0006("Can't bootstrap job"),

  FRAMEWORK_0007("Invalid execution engine"),

  FRAMEWORK_0008("Invalid combination of submission and execution engines"),

  ;

  private final String message;

  private FrameworkError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
