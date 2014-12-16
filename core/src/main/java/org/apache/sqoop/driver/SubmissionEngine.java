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
package org.apache.sqoop.driver;

import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.model.MSubmission;

/**
 * Submission engine is responsible in conveying the information about the
 * job instances (submissions) to remote (hadoop) cluster.
 */
public abstract class SubmissionEngine {

  /**
   * Initialize submission engine
   *
   * @param context Configuration context
   * @param prefix Submission engine prefix
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
  public abstract boolean isExecutionEngineSupported(Class<?> executionEngineClass);

  /**
   * Submit new job to remote (hadoop) cluster. This method *must* fill
   * submission.getSummary.setExternalId(), otherwise Sqoop won't
   * be able to track progress on this job!
   *
   * @return Return true if we were able to submit job to remote cluster.
   */
  public abstract boolean submit(JobRequest jobRequest);

  /**
   * Hard stop for given submission.
   *
   * @param externalJobId Submission external job id.
   */
  public abstract void stop(String externalJobId);

  /**
   * Update the given submission
   * @param submission record to update
   */
  public abstract void update(MSubmission submission);

}
