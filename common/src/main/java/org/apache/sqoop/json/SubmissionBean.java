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

import static org.apache.sqoop.json.util.SchemaSerialization.extractSchema;
import static org.apache.sqoop.json.util.SchemaSerialization.restoreSchema;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SubmissionBean implements JsonBean {

  private static final String SUBMISSION = "submission";
  private static final String JOB = "job";
  private static final String CREATION_USER = "creation-user";
  private static final String CREATION_DATE = "creation-date";
  private static final String LAST_UPDATE_USER = "last-udpate-user";
  private static final String LAST_UPDATE_DATE = "last-update-date";
  private static final String STATUS = "status";
  private static final String EXTERNAL_ID = "external-id";
  private static final String EXTERNAL_LINK = "external-link";
  private static final String ERROR_SUMMARY = "error-summary";
  private static final String ERROR_DETAILS = "error-details";
  private static final String PROGRESS = "progress";
  private static final String COUNTERS = "counters";
  private static final String FROM_SCHEMA = "from-schema";
  private static final String TO_SCHEMA = "to-schema";

  private List<MSubmission> submissions;

  public List<MSubmission> getSubmissions() {
    return submissions;
  }

  // For "extract"
  public SubmissionBean(MSubmission submission) {
    this();
    this.submissions = new ArrayList<MSubmission>();
    this.submissions.add(submission);
  }

  public SubmissionBean(List<MSubmission> submissions) {
    this();
    this.submissions = submissions;
  }

  // For "restore"
  public SubmissionBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONObject submission = new JSONObject();
    submission.put(SUBMISSION, extractSubmission(submissions.get(0)));
    return submission;
  }

  @SuppressWarnings("unchecked")
  protected JSONArray extractSubmissions() {
    JSONArray submissionsArray = new JSONArray();
    for (MSubmission submission : submissions) {
      submissionsArray.add(extractSubmission(submission));
    }
    return submissionsArray;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractSubmission(MSubmission submission) {
    JSONObject object = new JSONObject();

    object.put(JOB, submission.getJobId());
    object.put(STATUS, submission.getStatus().name());
    object.put(PROGRESS, submission.getProgress());

    if (submission.getCreationUser() != null) {
      object.put(CREATION_USER, submission.getCreationUser());
    }
    if (submission.getCreationDate() != null) {
      object.put(CREATION_DATE, submission.getCreationDate().getTime());
    }
    if (submission.getLastUpdateUser() != null) {
      object.put(LAST_UPDATE_USER, submission.getLastUpdateUser());
    }
    if (submission.getLastUpdateDate() != null) {
      object.put(LAST_UPDATE_DATE, submission.getLastUpdateDate().getTime());
    }
    if (submission.getExternalJobId() != null) {
      object.put(EXTERNAL_ID, submission.getExternalJobId());
    }
    if (submission.getExternalLink() != null) {
      object.put(EXTERNAL_LINK, submission.getExternalLink());
    }
    if (submission.getError().getErrorSummary() != null) {
      object.put(ERROR_SUMMARY, submission.getError().getErrorSummary());
    }
    if (submission.getError().getErrorDetails() != null) {
      object.put(ERROR_DETAILS, submission.getError().getErrorDetails());
    }
    if (submission.getCounters() != null) {
      object.put(COUNTERS, extractCounters(submission.getCounters()));
    }
    if (submission.getFromSchema() != null) {
      object.put(FROM_SCHEMA, extractSchema(submission.getFromSchema()));
    }
    if (submission.getToSchema() != null) {
      object.put(TO_SCHEMA, extractSchema(submission.getToSchema()));
    }
    return object;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractCounters(Counters counters) {
    JSONObject counterArray = new JSONObject();
    for (CounterGroup group : counters) {
      JSONObject counterGroup = new JSONObject();

      for (Counter counter : group) {
        counterGroup.put(counter.getName(), counter.getValue());
      }
      counterArray.put(group.getName(), counterGroup);
    }
    return counterArray;
  }

  @Override
  public void restore(JSONObject json) {
    submissions = new ArrayList<MSubmission>();
    JSONObject obj = (JSONObject) json.get(SUBMISSION);
    submissions.add(restoreSubmission(obj));
  }

  protected void restoreSubmissions(JSONArray array) {
    submissions = new ArrayList<MSubmission>();
    for (Object obj : array) {
      submissions.add(restoreSubmission(obj));
    }
  }

  private MSubmission restoreSubmission(Object obj) {
    JSONObject object = (JSONObject) obj;
    MSubmission submission = new MSubmission();
    submission.setJobId((Long) object.get(JOB));
    submission.setStatus(SubmissionStatus.valueOf((String) object.get(STATUS)));
    submission.setProgress((Double) object.get(PROGRESS));

    if (object.containsKey(CREATION_USER)) {
      submission.setCreationUser((String) object.get(CREATION_USER));
    }
    if (object.containsKey(CREATION_DATE)) {
      submission.setCreationDate(new Date((Long) object.get(CREATION_DATE)));
    }
    if (object.containsKey(LAST_UPDATE_USER)) {
      submission.setLastUpdateUser((String) object.get(LAST_UPDATE_USER));
    }
    if (object.containsKey(LAST_UPDATE_DATE)) {
      submission.setLastUpdateDate(new Date((Long) object.get(LAST_UPDATE_DATE)));
    }
    if (object.containsKey(EXTERNAL_ID)) {
      submission.setExternalJobId((String) object.get(EXTERNAL_ID));
    }
    if (object.containsKey(EXTERNAL_LINK)) {
      submission.setExternalLink((String) object.get(EXTERNAL_LINK));
    }
    if (object.containsKey(ERROR_SUMMARY)) {
      submission.getError().setErrorSummary((String) object.get(ERROR_SUMMARY));
    }
    if (object.containsKey(ERROR_DETAILS)) {
      submission.getError().setErrorDetails((String) object.get(ERROR_DETAILS));
    }
    if (object.containsKey(COUNTERS)) {
      submission.setCounters(restoreCounters((JSONObject) object.get(COUNTERS)));
    }

    if (object.containsKey(FROM_SCHEMA)) {
      submission.setFromSchema(restoreSchema((JSONObject) object.get(FROM_SCHEMA)));
    }
    if (object.containsKey(TO_SCHEMA)) {
      submission.setToSchema(restoreSchema((JSONObject) object.get(TO_SCHEMA)));
    }
    return submission;
  }

  @SuppressWarnings("unchecked")
  public Counters restoreCounters(JSONObject object) {
    Set<Map.Entry<String, JSONObject>> groupSet = object.entrySet();
    Counters counters = new Counters();

    for (Map.Entry<String, JSONObject> groupEntry : groupSet) {
      CounterGroup group = new CounterGroup(groupEntry.getKey());
      Set<Map.Entry<String, Long>> counterSet = groupEntry.getValue().entrySet();
      for (Map.Entry<String, Long> counterEntry : counterSet) {
        Counter counter = new Counter(counterEntry.getKey(), counterEntry.getValue());
        group.addCounter(counter);
      }
      counters.addCounterGroup(group);
    }
    return counters;
  }
}
