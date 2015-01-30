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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.ErrorCode;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum ConfigurableError implements ErrorCode {

  /** An unknown error has occurred. */
  CONFIGURABLE_0001("Link object upgrade called, but no upgrade routine provided for LINK config"),
  CONFIGURABLE_0002("Job object upgrade called, but no upgrade routine provided for FROM job config"),
  CONFIGURABLE_0003("Job object upgrade called, but no upgrade routine provided for TO job config"),
 ;
  private final String message;

  private ConfigurableError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}