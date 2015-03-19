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
package org.apache.sqoop.client;

import org.apache.sqoop.model.MSubmission;

/**
 * Callback interface for synchronous job submission
 */
public interface SubmissionCallback {

  /**
   * Invoked when job successfully submitted
   * @param submission
   */
  public void submitted(MSubmission submission);

  /**
   * Invoked with updated submission status when sqoop server is polled for update
   * @param submission
   */
  public void updated(MSubmission submission);

  /**
   * Invoked when job execution is finished
   * @param submission
   */
  public void finished(MSubmission submission);
}
