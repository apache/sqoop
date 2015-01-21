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
package org.apache.sqoop.json;

import java.util.List;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.model.MSubmission;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SubmissionsBean extends SubmissionBean {

  private static final String SUBMISSIONS = "submissions";

  // For "extract"
  public SubmissionsBean(MSubmission submission) {
    super(submission);
  }

  public SubmissionsBean(List<MSubmission> submissions) {
    super(submissions);

  }

  // For "restore"
  public SubmissionsBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray submissionsArray = super.extractSubmissions();
    JSONObject submissions = new JSONObject();
    submissions.put(SUBMISSIONS, submissionsArray);
    return submissions;
  }

  @Override
  public void restore(JSONObject json) {
    JSONArray submissionsArray = (JSONArray) json.get(SUBMISSIONS);
    restoreSubmissions(submissionsArray);
  }

}
