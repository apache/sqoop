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
package org.apache.sqoop.submission;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * List of states where the submission might be.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum SubmissionStatus {

  /**
   * In the middle of creating new submission. This might be creation step
   * on our side on remote cluster side.
   */
  BOOTING,

  /**
   * We weren't able to submit this submission to remote cluster
   */
  FAILURE_ON_SUBMIT,

  /**
   * Submission is running.
   */
  RUNNING,

  /**
   * Submission has finished gracefully
   */
  SUCCEEDED,

  /**
   * Submission has not finished gracefully, there were issues.
   */
  FAILED,

  /**
   * We have no idea in what state the submission actually is.
   */
  UNKNOWN,

  /**
   * Special submission type for job that was never executed.
   */
  NEVER_EXECUTED,

  ;

  /**
   * Return array of submission status that are considered as unfinished.
   *
   * @return Array of unfinished submission statuses
   */
  public static SubmissionStatus[] unfinished() {
    return new SubmissionStatus[] { RUNNING, BOOTING };
  }

  public boolean isRunning() {
    return this == RUNNING || this == BOOTING;
  }

  public boolean isFailure() {
    return this == FAILED || this == UNKNOWN || this == FAILURE_ON_SUBMIT;
  }
}
