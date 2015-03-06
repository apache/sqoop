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

/**
 *
 */
//TODO(https://issues.apache.org/jira/browse/SQOOP-1652): why is this called  Driver Error since it is used in JobManager?

public enum DriverError implements ErrorCode {

  DRIVER_0001("Invalid submission engine"),

  DRIVER_0002("Given job is already running"),

  DRIVER_0003("Given job is not running"),

  DRIVER_0004("Unknown job id"),

  DRIVER_0005("Unsupported job type"),

  DRIVER_0006("Can't bootstrap job"),

  DRIVER_0007("Invalid execution engine"),

  DRIVER_0008("Invalid combination of submission and execution engines"),

  //TODO(https://issues.apache.org/jira/browse/SQOOP-1652): address the submit/start terminology difference
  DRIVER_0009("Job has been disabled. Cannot submit this job."),

  DRIVER_0010("Link for this job has been disabled. Cannot submit this job."),

  DRIVER_0011("Connector does not support specified direction. Cannot submit this job."),

  ;

  private final String message;

  private DriverError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
