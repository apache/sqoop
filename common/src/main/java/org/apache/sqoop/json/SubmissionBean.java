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

import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.sqoop.json.util.SchemaSerialization.extractSchema;
import static org.apache.sqoop.json.util.SchemaSerialization.restoreSchemna;

/**
 *
 */
public class SubmissionBean implements JsonBean {

  private static final String ALL = "all";
  private static final String JOB = "job";
  private static final String CREATION_USER = "creation-user";
  private static final String CREATION_DATE = "creation-date";
  private static final String LAST_UPDATE_USER = "last-udpate-user";
  private static final String LAST_UPDATE_DATE = "last-update-date";
  private static final String STATUS = "status";
  private static final String EXTERNAL_ID = "external-id";
  private static final String EXTERNAL_LINK = "external-link";
  private static final String EXCEPTION = "exception";
  private static final String EXCEPTION_TRACE = "exception-trace";
  private static final String PROGRESS = "progress";
  private static final String COUNTERS = "counters";
  private static final String CONNECTOR_SCHEMA = "schema-connector";
  private static final String HIO_SCHEMA = "schema-hio";

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
    JSONArray array = new JSONArray();

    for(MSubmission submission : this.submissions) {
      JSONObject object = new JSONObject();

      object.put(JOB, submission.getJobId());
      object.put(STATUS, submission.getStatus().name());
      object.put(PROGRESS, submission.getProgress());

      if(submission.getCreationUser() != null) {
        object.put(CREATION_USER, submission.getCreationUser());
      }
      if(submission.getCreationDate() != null) {
        object.put(CREATION_DATE, submission.getCreationDate().getTime());
      }
      if(submission.getLastUpdateUser() != null) {
        object.put(LAST_UPDATE_USER, submission.getLastUpdateUser());
      }
      if(submission.getLastUpdateDate() != null) {
        object.put(LAST_UPDATE_DATE, submission.getLastUpdateDate().getTime());
      }
      if(submission.getExternalId() != null) {
        object.put(EXTERNAL_ID, submission.getExternalId());
      }
      if(submission.getExternalLink() != null) {
        object.put(EXTERNAL_LINK, submission.getExternalLink());
      }
      if(submission.getExceptionInfo() != null) {
        object.put(EXCEPTION, submission.getExceptionInfo());
      }
      if(submission.getExceptionStackTrace() != null) {
        object.put(EXCEPTION_TRACE, submission.getExceptionStackTrace());
      }
      if(submission.getCounters() != null) {
        object.put(COUNTERS, extractCounters(submission.getCounters()));
      }
      if(submission.getConnectorSchema() != null)  {
        object.put(CONNECTOR_SCHEMA, extractSchema(submission.getConnectorSchema()));
      }
      if(submission.getHioSchema() != null) {
        object.put(HIO_SCHEMA, extractSchema(submission.getHioSchema()));
      }

      array.add(object);
    }

    JSONObject all = new JSONObject();
    all.put(ALL, array);

    return all;
  }

  @SuppressWarnings("unchecked")
  public JSONObject extractCounters(Counters counters) {
    JSONObject ret = new JSONObject();
    for(CounterGroup group : counters) {
      JSONObject counterGroup = new JSONObject();

      for(Counter counter : group) {
        counterGroup.put(counter.getName(), counter.getValue());
      }

      ret.put(group.getName(), counterGroup);
    }
    return ret;
  }

  @Override
  public void restore(JSONObject json) {
    this.submissions = new ArrayList<MSubmission>();

    JSONArray array = (JSONArray) json.get(ALL);

    for (Object obj : array) {
      JSONObject object = (JSONObject) obj;
      MSubmission submission = new MSubmission();

      submission.setJobId((Long) object.get(JOB));
      submission.setStatus(SubmissionStatus.valueOf((String) object.get(STATUS)));
      submission.setProgress((Double) object.get(PROGRESS));

      if(object.containsKey(CREATION_USER)) {
        submission.setCreationUser((String) object.get(CREATION_USER));
      }
      if(object.containsKey(CREATION_DATE)) {
        submission.setCreationDate(new Date((Long) object.get(CREATION_DATE)));
      }
      if(object.containsKey(LAST_UPDATE_USER)) {
        submission.setLastUpdateUser((String) object.get(LAST_UPDATE_USER));
      }
      if(object.containsKey(LAST_UPDATE_DATE)) {
        submission.setLastUpdateDate(new Date((Long) object.get(LAST_UPDATE_DATE)));
      }
      if(object.containsKey(EXTERNAL_ID)) {
        submission.setExternalId((String) object.get(EXTERNAL_ID));
      }
      if(object.containsKey(EXTERNAL_LINK)) {
        submission.setExternalLink((String) object.get(EXTERNAL_LINK));
      }
      if(object.containsKey(EXCEPTION)) {
        submission.setExceptionInfo((String) object.get(EXCEPTION));
      }
      if(object.containsKey(EXCEPTION_TRACE)) {
        submission.setExceptionStackTrace((String) object.get(EXCEPTION_TRACE));
      }
      if(object.containsKey(COUNTERS)) {
        submission.setCounters(restoreCounters((JSONObject) object.get(COUNTERS)));
      }
      if(object.containsKey(CONNECTOR_SCHEMA)) {
        submission.setConnectorSchema(restoreSchemna((JSONObject) object.get(CONNECTOR_SCHEMA)));
      }
      if(object.containsKey(HIO_SCHEMA)) {
        submission.setHioSchema(restoreSchemna((JSONObject) object.get(HIO_SCHEMA)));
      }

      this.submissions.add(submission);
    }
  }

  public Counters restoreCounters(JSONObject object) {
    Set<Map.Entry<String, JSONObject>> groupSet = object.entrySet();
    Counters counters = new Counters();

    for(Map.Entry<String, JSONObject> groupEntry: groupSet) {

      CounterGroup group = new CounterGroup(groupEntry.getKey());

      Set<Map.Entry<String, Long>> counterSet = groupEntry.getValue().entrySet();

      for(Map.Entry<String, Long> counterEntry: counterSet) {
        Counter counter = new Counter(counterEntry.getKey(), counterEntry.getValue());
        group.addCounter(counter);
      }

      counters.addCounterGroup(group);
    }

    return counters;
  }
}
