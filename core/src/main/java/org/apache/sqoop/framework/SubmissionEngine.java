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
package org.apache.sqoop.framework;

import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.submission.SubmissionStatus;

/**
 * Submission engine is capable of executing and getting information about
 * submissions to remote (hadoop) cluster.
 */
public abstract class SubmissionEngine {

  /**
   * Initialize submission engine
   *
   * @param context Configuration context
   */
  public void initialize(MapContext context, String prefix) {
  }

  /**
   * Destroy submission engine when stopping server
   */
  public void destroy() {
  }

  /**
   * Callback to verify that configured submission engine and execution engine
   * are compatible.
   *
   * @param executionEngineClass Configured execution class.
   * @return True if such execution engine is supported
   */
  public abstract boolean isExecutionEngineSupported(Class executionEngineClass);

  /**
   * Submit new job to remote (hadoop) cluster. This method *must* fill
   * submission.getSummary.setExternalId(), otherwise Sqoop framework won't
   * be able to track progress on this job!
   *
   * @return Return true if we were able to submit job to remote cluster.
   */
  public abstract boolean submit(SubmissionRequest submission);

  /**
   * Hard stop for given submission.
   *
   * @param submissionId Submission internal id.
   */
  public abstract void stop(String submissionId);

  /**
   * Return status of given submission.
   *
   * @param submissionId Submission internal id.
   * @return Current submission status.
   */
  public abstract SubmissionStatus status(String submissionId);

  /**
   * Return submission progress.
   *
   * Expected is number from interval <0, 1> denoting how far the processing
   * has gone or -1 in case that this submission engine do not supports
   * progress reporting.
   *
   * @param submissionId Submission internal id.
   * @return {-1} union <0, 1>
   */
  public double progress(String submissionId) {
    return -1;
  }

  /**
   * Return statistics for given submission id.
   *
   * Sqoop framework will call counters only for submission in state SUCCEEDED,
   * it's consider exceptional state to call this method for other states.
   *
   * @param submissionId Submission internal id.
   * @return Submission statistics
   */
  public Counters counters(String submissionId) {
    return null;
  }

  /**
   * Return link to external web page with given submission.
   *
   * @param submissionId Submission internal id.
   * @return Null in case that external page is not supported or available or
   *  HTTP link to given submission.
   */
  public String externalLink(String submissionId) {
    return null;
  }
}
