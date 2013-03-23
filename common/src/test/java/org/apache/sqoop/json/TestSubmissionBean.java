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

import junit.framework.TestCase;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Date;

/**
 *
 */
public class TestSubmissionBean extends TestCase {

  public void testTransferUnknown() {
    transfer(MSubmission.UNKNOWN);
  }

  public void testTransferJobId() {
    MSubmission source = new MSubmission();
    source.setJobId(666);

    MSubmission target = transfer(source);
    assertEquals(666, target.getJobId());
  }

  public void testTransferCreationDate() {
    Date date = new Date();
    MSubmission source = new MSubmission();
    source.setCreationDate(date);

    MSubmission target = transfer(source);
    assertEquals(date, target.getCreationDate());
  }

  public void testTransferLastUpdateDate() {
    Date date = new Date();
    MSubmission source = new MSubmission();
    source.setLastUpdateDate(date);

    MSubmission target = transfer(source);
    assertEquals(date, target.getLastUpdateDate());
  }

  public void testTransferStatus() {
    MSubmission source = new MSubmission();
    source.setStatus(SubmissionStatus.SUCCEEDED);

    MSubmission target = transfer(source);
    assertEquals(SubmissionStatus.SUCCEEDED, target.getStatus());
  }

  public void testTransferExternalId() {
    MSubmission source = new MSubmission();
    source.setExternalId("Job-x");

    MSubmission target = transfer(source);
    assertEquals("Job-x", target.getExternalId());
  }

  public void testTransferExternalLink() {
    MSubmission source = new MSubmission();
    source.setExternalLink("http://");

    MSubmission target = transfer(source);
    assertEquals("http://", target.getExternalLink());
  }

  public void testTransferException() {
    MSubmission source = new MSubmission();
    source.setExceptionInfo("EndOfTheWorldException");

    MSubmission target = transfer(source);
    assertEquals("EndOfTheWorldException", target.getExceptionInfo());
  }

  public void testTransferExceptionTrace() {
    MSubmission source = new MSubmission();
    source.setExceptionStackTrace("void.java(3): line infinity");

    MSubmission target = transfer(source);
    assertEquals("void.java(3): line infinity", target.getExceptionStackTrace());
  }

  public void testTransferProgress() {
    MSubmission source = new MSubmission();
    source.setProgress(25.0);

    MSubmission target = transfer(source);
    assertEquals(25.0, target.getProgress());
  }

  public void testTransferCounters() {
    Counters counters = new Counters();
    counters.addCounterGroup(new CounterGroup("A")
      .addCounter(new Counter("X", 1))
      .addCounter(new Counter("Y", 2))
    );
    counters.addCounterGroup(new CounterGroup("B")
      .addCounter(new Counter("XX", 11))
      .addCounter(new Counter("YY", 22))
    );

    MSubmission source = new MSubmission();
    source.setCounters(counters);

    Counters target;
    CounterGroup group;
    Counter counter;

    target = transfer(source).getCounters();
    group = target.getCounterGroup("A");
    assertNotNull(group);
    counter = group.getCounter("X");
    assertNotNull(counter);
    assertEquals(1, counter.getValue());
    counter = group.getCounter("Y");
    assertNotNull(counter);
    assertEquals(2, counter.getValue());

    target = transfer(source).getCounters();
    group = target.getCounterGroup("B");
    assertNotNull(group);
    counter = group.getCounter("XX");
    assertNotNull(counter);
    assertEquals(11, counter.getValue());
    counter = group.getCounter("YY");
    assertNotNull(counter);
    assertEquals(22, counter.getValue());
  }

  /**
   * Simulate transfer of MSubmission structure using SubmissionBean
   *
   * @param submission Submission to transfer
   * @return
   */
  private MSubmission transfer(MSubmission submission) {
    SubmissionBean bean = new SubmissionBean(submission);
    JSONObject json = bean.extract(false);

    String string = json.toString();

    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    SubmissionBean retrievedBean = new SubmissionBean();
    retrievedBean.restore(retrievedJson);

    return retrievedBean.getSubmission();
  }

}

