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

  /** The system was unable to query the repository for metadata. */
  DERBYREPO_0004("Unable to retrieve metadata"),

  /** The metadata repository contains more than one connector with same name */
  DERBYREPO_0005("Invalid metadata state - multiple connectors with name"),

  /** The system does not support the given input type.*/
  DERBYREPO_0006("Unknown input type encountered"),

  /** The system does not support the given form type.*/
  DERBYREPO_0007("Unknown form type encountered"),

  /** No input metadata was found for the given form. */
  DERBYREPO_0008("The form contains no input metadata"),

  /** The system could not load the form due to unexpected position of input.*/
  DERBYREPO_0009("The form input retrieved does not match expected position"),

  /**
   * The system could not load the connector due to unexpected position
   * of form.
   */
  DERBYREPO_0010("The form retrieved does not match expected position"),

  /**
   * The system was not able to register metadata due to a pre-assigned
   * persistence identifier.
   */
  DERBYREPO_0011("Metadata cannot have preassigned persistence id"),

  /**
   * The system was unable to register various entities.
   */
  DERBYREPO_0012("Unexpected update count when registering entity"),

  /**
   * The system was unable to register metadata due to a failure to retrieve
   * the generated identifier.
   */
  DERBYREPO_0013("Unable to retrieve generated identifier"),

  /**
   * The system was unable to register connector metadata due to a server
   * error.
   */
  DERBYREPO_0014("Registration of connector metadata failed"),

  /**
   * The system was not able to register connector metadata due to an unexpected
   * update count.
   */
  DERBYREPO_0015("Unexpected update count on form registration"),

  /**
   * The system was unable to register connector metadata due to a failure to
   * retrieve the generated identifier for a form.
   */
  DERBYREPO_0016("Unable to retrieve generated identifier for form"),

  /**
   * The system was unable to register connector metadata due to an unexpected
   * update count for form input registration.
   */
  DERBYREPO_0017("Unexpected update count for form input"),

  /**
   * The system was unable to register connector metadata due to a failure to
   * retrieve the generated identifier for a form input.
   */
  DERBYREPO_0018("Unable to retrieve generated identifier for form input"),

  /** We cant create new connection in metastore **/
  DERBYREPO_0019("Unable to create new connection data"),

  /** We can't save values for input to metastore **/
  DERBYREPO_0020("Unable to save input values to metadata repository"),

  /** We can't update connection in metastore **/
  DERBYREPO_0021("Unable to update connection metadata in repository"),

  /** We can't delete connection in metastore **/
  DERBYREPO_0022("Unable to delete connection metadata in repository"),

  /** We can't restore connection metadata from metastore **/
  DERBYREPO_0023("Unable to load connection metadata from repository"),

  /** We can't restore specific connection metadata from metastore **/
  DERBYREPO_0024("Unable to load specific connection metadata from repository"),

  /** We're unable to check if given connection already exists */
  DERBYREPO_0025("Unable to check if given connection exists"),

  /** We cant create new job in metastore **/
  DERBYREPO_0026("Unable to create new job data"),

  /** We can't update job in metastore **/
  DERBYREPO_0027("Unable to update job metadata in repository"),

  /** We can't delete job in metastore **/
  DERBYREPO_0028("Unable to delete job metadata in repository"),

  /** We're unable to check if given job already exists */
  DERBYREPO_0029("Unable to check if given job exists"),

  /** We can't restore specific job metadata from metastore **/
  DERBYREPO_0030("Unable to load specific job metadata from repository"),

  /** We can't restore job metadata from metastore **/
  DERBYREPO_0031("Unable to load job metadata from repository"),

  /** Can't verify if connection is referenced from somewhere **/
  DERBYREPO_0032("Unable to check if connection is in use"),

  /** We're unable to check if given submission already exists */
  DERBYREPO_0033("Unable to check if given submission exists"),

  /** We cant create new submission in metastore **/
  DERBYREPO_0034("Unable to create new submission data"),

  /** We can't update submission in metastore **/
  DERBYREPO_0035("Unable to update submission metadata in repository"),

  /** Can't purge old submissions **/
  DERBYREPO_0036("Unable to purge old submissions"),

  /** Can't retrieve unfinished submissions **/
  DERBYREPO_0037("Can't retrieve unfinished submissions"),

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
