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
package org.apache.sqoop.repository;

import org.apache.sqoop.common.ErrorCode;

public enum RepositoryError implements ErrorCode {

  // General Repository Errors: Prefix REPO

  /** An unknown error has occurred. */
  REPO_0000("An unknown error has occurred"),

  /** The system was unable to find or load the repository provider. */
  REPO_0001("Invalid repository provider specified"),

  // JDBC Repository Errors: Prefix JDBCREP

  /** An unknown error has occurred. */
  JDBCREPO_0000("An unknown error has occurred"),

  /** The system was unable to find or load the JDBC repository handler. */
  JDBCREPO_0001("Invalid JDBC Repository Handler specified"),

  /** An invalid JDBC connection URL was specified. */
  JDBCREPO_0002("Invalid JDBC connection URL specified"),

  /** An invalid JDBC driver class name was specified. */
  JDBCREPO_0003("Invalid JDBC driver class specified"),

  /** An invalid JDBC transaction isolation level was specified. */
  JDBCREPO_0004("Invalid JDBC transaction isolation level specified"),

  /** The value specified for maximum connection pool connections is invalid.*/
  JDBCREPO_0005("Invalid maximum connections specified for connection pool"),

  /** The system attempted to use an inactive transaction. */
  JDBCREPO_0006("Transaction is not active"),

  /**
   * The system was unable to obtain a connection lease for the
   * requested transaction.
   */
  JDBCREPO_0007("Unable to lease connection"),

  /** The system attempted to commit a transaction marked for rollback.*/
  JDBCREPO_0008("Attempt to commit a transaction marked for rollback"),

  /** The system was unable to finalize the transaction. */
  JDBCREPO_0009("Failed to finalize transaction"),

  /** The system was not able to deregister the driver during shutdown. */
  JDBCREPO_0010("Unable to deregister driver during shutdown"),

  /**
   * An attempt was made to reinitialize already
   * initialized JDBC repository context.
   */
  JDBCREPO_0011("Attempt to reinitialize JDBC repository context"),

  /** Failure in metadata repository operation. */
  JDBCREPO_0012("Failure in metadata repository operation."),

  /** The system found a change in connector metadata that requires upgrade. */
  JDBCREPO_0013("Connector metadata changed - upgrade may be required"),

  /** The system found a change in framework metadata that requires upgrade. */
  JDBCREPO_0014("Framework metadata changed - upgrade may be required"),

  /** Connection that we're trying to create is already saved in repository **/
  JDBCREPO_0015("Cannot create connection that was already created"),

  /** Connection that we're trying to update is not yet saved **/
  JDBCREPO_0016("Cannot update connection that was not yet created"),

  /** Invalid connection id **/
  JDBCREPO_0017("Given connection id is invalid"),

  /** Job that we're trying to create is already saved in repository **/
  JDBCREPO_0018("Cannot create job that was already created"),

  /** Job that we're trying to update is not yet saved **/
  JDBCREPO_0019("Cannot update job that was not yet created"),

  /** Invalid job id **/
  JDBCREPO_0020("Given job id is invalid"),

  /** Connection ID is in use **/
  JDBCREPO_0021("Given connection id is in use"),

  /** Job ID is in use **/
  JDBCREPO_0022("Given job id is in use"),

  /** Cannot create submission that was already created **/
  JDBCREPO_0023("Cannot create submission that was already created"),

  /** Submission that we're trying to update is not yet created **/
  JDBCREPO_0024("Cannot update submission that was not yet created"),

  /** Invalid submission id **/
  JDBCREPO_0025("Given submission id is invalid"),

  ;

  private final String message;

  private RepositoryError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
