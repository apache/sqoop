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

public enum CommonRepositoryError implements ErrorCode {
  // JDBC Repository Handler Errors: Prefix COMMON

  /** The system was unable to run the specified query. */
  COMMON_0000("Unable to run specified query"),

  /** The system was unable to query the repository for given entity */
  COMMON_0001("Unable to retrieve entity data"),

  /** The repository contains more than one connector with same name */
  COMMON_0002("Invalid entity state - multiple connectors with name"),

  /** The system does not support the given input type.*/
  COMMON_0003("Unknown input type encountered"),

  /** The system does not support the given config type.*/
  COMMON_0004("Unknown config type encountered"),

  /** No input was found for the given config. */
  COMMON_0005("The config contains no input"),

  /** The system could not load the config due to unexpected position of input.*/
  COMMON_0006("The config input retrieved does not match expected position"),

  /**
   * The system could not load the connector due to unexpected position
   * of config.
   */
  COMMON_0007("The config retrieved does not match expected position"),

  /**
   * The system was not able to register entity due to a pre-assigned
   * persistence identifier.
   */
  COMMON_0008("Entity cannot have preassigned persistence id"),

  /**
   * The system was unable to register various entities.
   */
  COMMON_0009("Unexpected update count when registering entity"),

  /**
   * The system was unable to register entity due to a failure to retrieve
   * the generated identifier.
   */
  COMMON_0010("Unable to retrieve generated identifier"),

  /**
   * The system was unable to register connector due to a server
   * error.
   */
  COMMON_0011("Registration of connector failed"),

  /**
   * The system was not able to register connector due to an unexpected
   * update count.
   */
  COMMON_0012("Unexpected update count on config registration"),

  /**
   * The system was unable to register connector due to a failure to
   * retrieve the generated identifier for a config.
   */
  COMMON_0013("Unable to retrieve generated identifier for config"),

  /**
   * The system was unable to register connector due to an unexpected
   * update count for config input registration.
   */
  COMMON_0014("Unexpected update count for config input"),

  /**
   * The system was unable to register connector due to a failure to
   * retrieve the generated identifier for a config input.
   */
  COMMON_0015("Unable to retrieve generated identifier for config input"),

  /** We cant create new link in repository **/
  COMMON_0016("Unable to create new link data"),

  /** We can't save values for input to repository **/
  COMMON_0017("Unable to save input values to the repository"),

  /** We can't update link in repository **/
  COMMON_0018("Unable to update link in repository"),

  /** We can't delete link in repository **/
  COMMON_0019("Unable to delete link in repository"),

  /** We can't restore link from repository **/
  COMMON_0020("Unable to load link from repository"),

  /** We can't restore specific link from repository **/
  COMMON_0021("Unable to load specific link from repository"),

  /** We're unable to check if given link already exists */
  COMMON_0022("Unable to check if given link exists"),

  /** We cant create new job in repository **/
  COMMON_0023("Unable to create new job data"),

  /** We can't update job in repository **/
  COMMON_0024("Unable to update job in repository"),

  /** We can't delete job in repository **/
  COMMON_0025("Unable to delete job in repository"),

  /** We're unable to check if given job already exists */
  COMMON_0026("Unable to check if given job exists"),

  /** We can't restore specific job from repository **/
  COMMON_0027("Unable to load specific job from repository"),

  /** We can't restore job from repository **/
  COMMON_0028("Unable to load job from repository"),

  /** Can't verify if link is referenced from somewhere **/
  COMMON_0029("Unable to check if link is in use"),

  /** We're unable to check if given submission already exists */
  COMMON_0030("Unable to check if given submission exists"),

  /** We cant create new submission in repository **/
  COMMON_0031("Unable to create new submission data"),

  /** We can't update submission in repository **/
  COMMON_0032("Unable to update submission in the repository"),

  /** Can't purge old submissions **/
  COMMON_0033("Unable to purge old submissions"),

  /** Can't retrieve unfinished submissions **/
  COMMON_0034("Can't retrieve unfinished submissions"),

  /** Can't update connector **/
  COMMON_0035("Update of connector failed"),

  /** Can't retrieve all submissions **/
  COMMON_0036("Can't retrieve all submissions"),

  /** Can't retrieve submissions for a job **/
  COMMON_0037("Can't retrieve submissions for a job"),

  /** Can't enable/disable link **/
  COMMON_0038("Can't enable/disable link"),

  /** Can't enable/disable job **/
  COMMON_0039("Can't enable/disable job"),

  /** Can't update driver config **/
  COMMON_0040("Update of driver config failed"),

  /** Can't retrieve all connectors **/
  COMMON_0041("Can't retrieve all connectors"),

  /** Can't register config direction **/
  COMMON_0042("Could not register config direction"),

  /** Can't set connector direction **/
  COMMON_0043("Could not set connector direction"),

  /** The system was unable to register driver due to a server error **/
  COMMON_0044("Registration of driver failed"),

  /**
   * Config Input that is set to USER_ONLY editable cannot override other
   * USER_ONLY input
   **/
  COMMON_0045("Config Input cannot override USER_ONLY attribute"),

  /** Config Input cannot override itself */
  COMMON_0046("Config Input cannot override itself"),

  COMMON_0047("Config Input relation insertion failed"),

  COMMON_0048("Config Input overrides could not be fetched"),

  COMMON_0049("Unable to fetch FROM job config"),

  COMMON_0050("Unable to fetch TO job config"),

  COMMON_0051("Unable to fetch DRIVER job config"),

  COMMON_0052("Unable to fetch LINK config"),

  COMMON_0053("Unable to update job config"),

  COMMON_0054("Unable to update link config"),

  COMMON_0055("Unable to update CONNECTOR_ONLY editable config"),

  COMMON_0056("Unable to update  USER_ONLY editable config"),

  /** We can't restore specific connector**/
  COMMON_0057("Unable to load specific connector"),

  ;

  private final String message;

  private CommonRepositoryError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
