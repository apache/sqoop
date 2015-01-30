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

public enum PostgresqlRepoError implements ErrorCode {

  POSTGRESQLREPO_0000("An unknown error has occurred"),

  POSTGRESQLREPO_0001("Unable to run specified query"),

  POSTGRESQLREPO_0002("Update of driver config failed"),

  POSTGRESQLREPO_0003("Could not add directions"),

  POSTGRESQLREPO_0004("Could not get ID of recently added direction"),

  POSTGRESQLREPO_0005("Unsupported repository version"),

  ;

  private final String message;

  private PostgresqlRepoError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
