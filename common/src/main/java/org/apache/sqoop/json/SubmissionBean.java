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
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SubmissionBean implements JsonBean {

  private static final String JOB = "job";
  private static final String CREATION_DATE = "creation-date";
  private static final String LAST_UPDATE_DATE = "last-update-date";
  private static final String STATUS = "status";
  private static final String EXTERNAL_ID = "external-id";
  private static final String EXTERNAL_LINK = "external-link";
  private static final String EXCEPTION = "exception";
  private static final String EXCEPTION_TRACE = "exception-trace";
  private static final String PROGRESS = "progress";
  private static final String COUNTERS = "counters";

  private MSubmission submission;

  public MSubmission getSubmission() {
    return submission;
  }

  // For "extract"
  public SubmissionBean(MSubmission submission) {
    this.submission = submission;
  }

  // For "restore"
  public SubmissionBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONObject ret = new JSONObject();

    ret.put(JOB, submission.getJobId());
    ret.put(STATUS, submission.getStatus().name());
    ret.put(PROGRESS, submission.getProgress());

    if(submission.getCreationDate() != null) {
      ret.put(CREATION_DATE, submission.getCreationDate().getTime());
    }
    if(submission.getLastUpdateDate() != null) {
      ret.put(LAST_UPDATE_DATE, submission.getLastUpdateDate().getTime());
    }
    if(submission.getExternalId() != null) {
      ret.put(EXTERNAL_ID, submission.getExternalId());
    }
    if(submission.getExternalLink() != null) {
      ret.put(EXTERNAL_LINK, submission.getExternalLink());
    }
    if(submission.getExceptionInfo() != null) {
      ret.put(EXCEPTION, submission.getExceptionInfo());
    }
    if(submission.getExceptionStackTrace() != null) {
      ret.put(EXCEPTION_TRACE, submission.getExceptionStackTrace());
    }
    if(submission.getCounters() != null) {
      ret.put(COUNTERS, extractCounters(submission.getCounters()));
    }

    return ret;
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

    submission = new MSubmission();
    submission.setJobId((Long) json.get(JOB));
    submission.setStatus(SubmissionStatus.valueOf((String) json.get(STATUS)));
    submission.setProgress((Double) json.get(PROGRESS));

    if(json.containsKey(CREATION_DATE)) {
      submission.setCreationDate(new Date((Long) json.get(CREATION_DATE)));
    }
    if(json.containsKey(LAST_UPDATE_DATE)) {
      submission.setLastUpdateDate(new Date((Long) json.get(LAST_UPDATE_DATE)));
    }
    if(json.containsKey(EXTERNAL_ID)) {
      submission.setExternalId((String) json.get(EXTERNAL_ID));
    }
    if(json.containsKey(EXTERNAL_LINK)) {
      submission.setExternalLink((String) json.get(EXTERNAL_LINK));
    }
    if(json.containsKey(EXCEPTION)) {
      submission.setExceptionInfo((String) json.get(EXCEPTION));
    }
    if(json.containsKey(EXCEPTION_TRACE)) {
      submission.setExceptionStackTrace((String) json.get(EXCEPTION_TRACE));
    }
    if(json.containsKey(COUNTERS)) {
      submission.setCounters(restoreCounters((JSONObject) json.get(COUNTERS)));
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
