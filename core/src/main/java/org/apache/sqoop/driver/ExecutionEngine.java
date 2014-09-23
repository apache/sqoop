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

import org.apache.sqoop.common.ImmutableContext;

/**
 * Execution engine drives execution of sqoop job. It's responsible
 * for executing all defined steps in the import/export workflow.
 * A successful job execution will be recorded in the job submission entity
 */
public abstract class ExecutionEngine {

  /**
   * Initialize execution engine
   *
   * @param context Configuration context
   * @parma prefix Execution engine prefix
   */
  public void initialize(ImmutableContext context, String prefix) {
  }

  /**
   * Destroy execution engine when stopping server
   */
  public void destroy() {
  }

  /**
   * Return new JobRequest class or any subclass if it's needed by
   * execution and submission engine combination.
   *
   * @return new JobRequestobject
   */
  public JobRequest createJobRequest() {
    return new JobRequest();
  }

  /**
   * Prepare given job request.
   *
   * @param request JobRequest
   */
  public abstract void prepareJob(JobRequest request);
}
