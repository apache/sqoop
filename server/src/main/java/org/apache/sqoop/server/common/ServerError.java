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
package org.apache.sqoop.server.common;

import org.apache.sqoop.common.ErrorCode;

/**
 *
 */
public enum ServerError implements ErrorCode {

  /** Unknown error on server side. */
  SERVER_0001("Unknown server error"),

  /** Unknown error on server side. */
  SERVER_0002("Unsupported HTTP method"),

  /** We've received invalid HTTP request */
  SERVER_0003("Invalid HTTP request"),

  /** Invalid argument in HTTP request */
  SERVER_0004("Invalid argument in HTTP request"),

  /** Invalid entity requested */
  SERVER_0005("Invalid entity requested"),

  ;

  private final String message;

  private ServerError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
