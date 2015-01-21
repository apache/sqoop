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
package org.apache.sqoop.model;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 *Represents the job submission error
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SubmissionError {

  /**
   * Associated error (exception or failure) summary with this job (if any).
   *
   * This property is optional.
   */
  String errorSummary;

  /**
   * Associated error details, possibly the stacktrace with this job (if any).
   *
   * This property is optional.
   */
  String errorDetails;

  /**
   * @return the errorSummary
   */
  public String getErrorSummary() {
    return errorSummary;
  }

  /**
   * @param errorSummary
   *          the errorSummary to set
   */
  public void setErrorSummary(String errorSummary) {
    this.errorSummary = errorSummary;
  }

  /**
   * @return the errorDetails
   */
  public String getErrorDetails() {
    return errorDetails;
  }

  /**
   * @param errorDetails
   *          the errorDetails to set
   */
  public void setErrorDetails(String errorDetails) {
    this.errorDetails = errorDetails;
  }

  @Override
  public String toString() {
    return "JobSubmissionError [errorSummary=" + errorSummary + ", errorDetails=" + errorDetails
        + "]";
  }

}