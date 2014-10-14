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
package org.apache.sqoop.repository.derby;

import org.apache.sqoop.common.ErrorCode;

public enum DerbyRepoError implements ErrorCode {

  /** An unknown error has occurred. */
  DERBYREPO_0000("An unknown error has occurred"),

  /** The Derby Repository handler was unable to determine if schema exists.*/
  DERBYREPO_0001("Unable to determine if schema exists"),

  /** The system was unable to shutdown embedded derby repository server. */
  DERBYREPO_0002("Unable to shutdown embedded  Derby instance"),

  /** The system was unable to run the specified query. */
  DERBYREPO_0003("Unable to run specified query"),

  /** The system was unable to query the repository for given entity */
  DERBYREPO_0004("Unable to retrieve entity data"),

  /** The repository contains more than one connector with same name */
  DERBYREPO_0005("Invalid entity state - multiple connectors with name"),

  /** The system does not support the given input type.*/
  DERBYREPO_0006("Unknown input type encountered"),

  /** The system does not support the given config type.*/
  DERBYREPO_0007("Unknown config type encountered"),

  /** No input was found for the given config. */
  DERBYREPO_0008("The config contains no input"),

  /** The system could not load the config due to unexpected position of input.*/
  DERBYREPO_0009("The config input retrieved does not match expected position"),

  /**
   * The system could not load the connector due to unexpected position
   * of config.
   */
  DERBYREPO_0010("The config retrieved does not match expected position"),

  /**
   * The system was not able to register entity due to a pre-assigned
   * persistence identifier.
   */
  DERBYREPO_0011("Entity cannot have preassigned persistence id"),

  /**
   * The system was unable to register various entities.
   */
  DERBYREPO_0012("Unexpected update count when registering entity"),

  /**
   * The system was unable to register entity due to a failure to retrieve
   * the generated identifier.
   */
  DERBYREPO_0013("Unable to retrieve generated identifier"),

  /**
   * The system was unable to register connector due to a server
   * error.
   */
  DERBYREPO_0014("Registration of connector failed"),

  /**
   * The system was not able to register connector due to an unexpected
   * update count.
   */
  DERBYREPO_0015("Unexpected update count on config registration"),

  /**
   * The system was unable to register connector due to a failure to
   * retrieve the generated identifier for a config.
   */
  DERBYREPO_0016("Unable to retrieve generated identifier for config"),

  /**
   * The system was unable to register connector due to an unexpected
   * update count for config input registration.
   */
  DERBYREPO_0017("Unexpected update count for config input"),

  /**
   * The system was unable to register connector due to a failure to
   * retrieve the generated identifier for a config input.
   */
  DERBYREPO_0018("Unable to retrieve generated identifier for config input"),

  /** We cant create new link in repository **/
  DERBYREPO_0019("Unable to create new link data"),

  /** We can't save values for input to repository **/
  DERBYREPO_0020("Unable to save input values to the repository"),

  /** We can't update link in repository **/
  DERBYREPO_0021("Unable to update link in repository"),

  /** We can't delete link in repository **/
  DERBYREPO_0022("Unable to delete link in repository"),

  /** We can't restore link from repository **/
  DERBYREPO_0023("Unable to load link from repository"),

  /** We can't restore specific link from repository **/
  DERBYREPO_0024("Unable to load specific link from repository"),

  /** We're unable to check if given link already exists */
  DERBYREPO_0025("Unable to check if given link exists"),

  /** We cant create new job in repository **/
  DERBYREPO_0026("Unable to create new job data"),

  /** We can't update job in repository **/
  DERBYREPO_0027("Unable to update job in repository"),

  /** We can't delete job in repository **/
  DERBYREPO_0028("Unable to delete job in repository"),

  /** We're unable to check if given job already exists */
  DERBYREPO_0029("Unable to check if given job exists"),

  /** We can't restore specific job from repository **/
  DERBYREPO_0030("Unable to load specific job from repository"),

  /** We can't restore job from repository **/
  DERBYREPO_0031("Unable to load job from repository"),

  /** Can't verify if link is referenced from somewhere **/
  DERBYREPO_0032("Unable to check if link is in use"),

  /** We're unable to check if given submission already exists */
  DERBYREPO_0033("Unable to check if given submission exists"),

  /** We cant create new submission in repository **/
  DERBYREPO_0034("Unable to create new submission data"),

  /** We can't update submission in repository **/
  DERBYREPO_0035("Unable to update submission in the repository"),

  /** Can't purge old submissions **/
  DERBYREPO_0036("Unable to purge old submissions"),

  /** Can't retrieve unfinished submissions **/
  DERBYREPO_0037("Can't retrieve unfinished submissions"),

  DERBYREPO_0038("Update of connector failed"),

  /** Can't retrieve all submissions **/
  DERBYREPO_0039("Can't retrieve all submissions"),

  /** Can't retrieve submissions for a job **/
  DERBYREPO_0040("Can't retrieve submissions for a job"),

  /** Can't detect version of the database structures **/
  DERBYREPO_0041("Can't detect version of repository storage"),

  /** Can't enable/disable link **/
  DERBYREPO_0042("Can't enable/disable link"),

  /** Can't enable/disable job **/
  DERBYREPO_0043("Can't enable/disable job"),

  DERBYREPO_0044("Update of driver config failed"),

  DERBYREPO_0045("Can't retrieve all connectors"),

  DERBYREPO_0046("Could not add directions"),

  DERBYREPO_0047("Could not get ID of recently added direction"),

  DERBYREPO_0048("Could not register config direction"),

  DERBYREPO_0049("Could not set connector direction")
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