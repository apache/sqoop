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

public enum DerbyRepoError implements ErrorCode {

  /** An unknown error has occurred. */
  DERBYREPO_0000("An unknown error has occurred"),

  /** The system was unable to shutdown embedded derby repository server. */
  DERBYREPO_0001("Unable to shutdown embedded  Derby instance"),

  /** The system was unable to run the specified query. */
  DERBYREPO_0002("Unable to run specified query"),

  /**
   * The system was unable to register various entities.
   */
  DERBYREPO_0003("Unexpected update count when registering entity"),

  /**
   * The system was unable to register entity due to a failure to retrieve
   * the generated identifier.
   */
  DERBYREPO_0004("Unable to retrieve generated identifier"),

  /** We cant create new link in repository **/
  DERBYREPO_0005("Unable to create new link data"),

  /** Can't detect version of the database structures **/
  DERBYREPO_0006("Can't detect version of repository storage"),

  /** Can't add directions **/
  DERBYREPO_0007("Could not add directions"),

  /** Can't get ID of direction **/
  DERBYREPO_0008("Could not get ID of recently added direction"),

  /** The system was unable to register driver due to a server error **/
  DERBYREPO_0009("Registration of driver failed"),

  ;

  private final String message;

  private DerbyRepoError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}