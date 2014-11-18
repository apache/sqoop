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
package org.apache.sqoop.client;

import org.apache.sqoop.common.ErrorCode;

public enum ClientError implements ErrorCode {

  /** An unknown error has occurred. */
  CLIENT_0000("An unknown error has occurred"),

  /** There occurred exception on server side **/
  CLIENT_0001("Server has returned exception"),

  /** Polling time of submission status cannot be negative */
  CLIENT_0002("Polling time of submission status cannot be negative"),

  /** Given connector is not known to the server **/
  CLIENT_0003("Connector do not exists"),

  /** The system was not able to find valid Kerberos tikcet cache (kinit). */
  CLIENT_0004("Unable to find valid Kerberos ticket cache (kinit)");
  ;

  private final String message;

  private ClientError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
