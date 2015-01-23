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

import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNotNull;

// NOTE: This tests both the submission and submissions list bean
public class TestSubmissionBean {

  private static final double EPSILON = 0.01;

  @Test
  public void testTransferUnknown() {
    transfer(MSubmission.UNKNOWN);

    List<MSubmission> submissions = new ArrayList<MSubmission>();
    submissions.add(MSubmission.UNKNOWN);
    submissions.add(MSubmission.UNKNOWN);
    transfer(submissions);
  }

  @Test
  public void testTransferJobId() {
    MSubmission source = new MSubmission();
    source.setJobId(666);

    MSubmission target = transfer(source);
    assertEquals(666, target.getJobId());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setJobId(777);
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setJobId(888);
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals(777, targets.get(0).getJobId());
    assertNotNull(targets.get(1));
    assertEquals(888, targets.get(1).getJobId());
  }

  @Test
  public void testTransferCreationUser() {
    String username = "admin";
    MSubmission source = new MSubmission();
    source.setCreationUser(username);

    MSubmission target = transfer(source);
    assertEquals("admin", target.getCreationUser());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setCreationUser("userA");
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setCreationUser("userB");
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals("userA", targets.get(0).getCreationUser());
    assertNotNull(targets.get(1));
    assertEquals("userB", targets.get(1).getCreationUser());
  }

  @Test
  public void testTransferCreationDate() {
    Date date = new Date();
    MSubmission source = new MSubmission();
    source.setCreationDate(date);

    MSubmission target = transfer(source);
    assertEquals(date, target.getCreationDate());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    Date datex = new Date(1000);
    sourcex.setCreationDate(datex);
    sources.add(sourcex);
    Date datey = new Date(2000);
    MSubmission sourcey = new MSubmission();
    sourcey.setCreationDate(datey);
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals(datex, targets.get(0).getCreationDate());
    assertNotNull(targets.get(1));
    assertEquals(datey, targets.get(1).getCreationDate());
  }

  @Test
  public void testTransferLastUpdateUser() {
    String username = "admin";
    MSubmission source = new MSubmission();
    source.setLastUpdateUser(username);

    MSubmission target = transfer(source);
    assertEquals("admin", target.getLastUpdateUser());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setLastUpdateUser("userA");
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setLastUpdateUser("userB");
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals("userA", targets.get(0).getLastUpdateUser());
    assertNotNull(targets.get(1));
    assertEquals("userB", targets.get(1).getLastUpdateUser());
  }

  @Test
  public void testTransferLastUpdateDate() {
    Date date = new Date();
    MSubmission source = new MSubmission();
    source.setLastUpdateDate(date);

    MSubmission target = transfer(source);
    assertEquals(date, target.getLastUpdateDate());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    Date datex = new Date(1000);
    sourcex.setLastUpdateDate(datex);
    sources.add(sourcex);
    Date datey = new Date(2000);
    MSubmission sourcey = new MSubmission();
    sourcey.setLastUpdateDate(datey);
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals(datex, targets.get(0).getLastUpdateDate());
    assertNotNull(targets.get(1));
    assertEquals(datey, targets.get(1).getLastUpdateDate());
  }

  @Test
  public void testTransferStatus() {
    MSubmission source = new MSubmission();
    source.setStatus(SubmissionStatus.SUCCEEDED);

    MSubmission target = transfer(source);
    assertEquals(SubmissionStatus.SUCCEEDED, target.getStatus());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setStatus(SubmissionStatus.RUNNING);
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setStatus(SubmissionStatus.BOOTING);
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals(SubmissionStatus.RUNNING, targets.get(0).getStatus());
    assertNotNull(targets.get(1));
    assertEquals(SubmissionStatus.BOOTING, targets.get(1).getStatus());
  }

  @Test
  public void testTransferExternalId() {
    MSubmission source = new MSubmission();
    source.setExternalJobId("Job-x");

    MSubmission target = transfer(source);
    assertEquals("Job-x", target.getExternalJobId());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setExternalJobId("Job-y");
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setExternalJobId("Job-z");
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals("Job-y", targets.get(0).getExternalJobId());
    assertNotNull(targets.get(1));
    assertEquals("Job-z", targets.get(1).getExternalJobId());
  }

  @Test
  public void testTransferExternalLink() {
    MSubmission source = new MSubmission();
    source.setExternalLink("http://");

    MSubmission target = transfer(source);
    assertEquals("http://", target.getExternalLink());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setExternalLink("http://localhost:80");
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setExternalLink("http://localhost:8080");
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals("http://localhost:80", targets.get(0).getExternalLink());
    assertNotNull(targets.get(1));
    assertEquals("http://localhost:8080", targets.get(1).getExternalLink());
  }

  @Test
  public void testTransferErrorSummary() {
    SubmissionError error = new SubmissionError();
    MSubmission source = new MSubmission();
    error.setErrorSummary("EndOfTheWorldException");
    source.setError(error);
    MSubmission target = transfer(source);
    assertEquals("EndOfTheWorldException", target.getError().getErrorSummary());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    SubmissionError errorx= new SubmissionError();
    errorx.setErrorSummary("TheNewEraException");
    sourcex.setError(errorx);
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    SubmissionError errory= new SubmissionError();
    errory.setErrorSummary("EndOfTheWorldAgainException");
    sourcey.setError(errory);
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals("TheNewEraException", targets.get(0).getError().getErrorSummary());
    assertNotNull(targets.get(1));
    assertEquals("EndOfTheWorldAgainException", targets.get(1).getError().getErrorSummary());
  }

  @Test
  public void testTransferErrorDetails() {
    MSubmission source = new MSubmission();
    source.getError().setErrorDetails("void.java(3): line infinity");

    MSubmission target = transfer(source);
    assertEquals("void.java(3): line infinity", target.getError().getErrorDetails());

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.getError().setErrorDetails("void.java(4): segment fault in Java");
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.getError().setErrorDetails("void.java(5): core dumps in Java");
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals("void.java(4): segment fault in Java", targets.get(0).getError().getErrorDetails());
    assertNotNull(targets.get(1));
    assertEquals("void.java(5): core dumps in Java", targets.get(1).getError().getErrorDetails());
  }

  @Test
  public void testTransferProgress() {
    MSubmission source = new MSubmission();
    source.setProgress(25.0);

    MSubmission target = transfer(source);
    assertEquals(25.0, target.getProgress(), EPSILON);

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setProgress(50.0);
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setProgress(99.9);
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    assertEquals(50.0, targets.get(0).getProgress(), EPSILON);
    assertNotNull(targets.get(1));
    assertEquals(99.9, targets.get(1).getProgress(), EPSILON);
  }

  @Test
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

    Counters countersx = new Counters();
    countersx.addCounterGroup(new CounterGroup("C")
      .addCounter(new Counter("XXX", 111))
      .addCounter(new Counter("YYY", 222))
    );
    countersx.addCounterGroup(new CounterGroup("D")
      .addCounter(new Counter("XXXX", 1111))
      .addCounter(new Counter("YYYY", 2222))
    );

    Counters countersy = new Counters();
    countersy.addCounterGroup(new CounterGroup("E")
      .addCounter(new Counter("XXXXX", 11111))
      .addCounter(new Counter("YYYYY", 22222))
    );
    countersy.addCounterGroup(new CounterGroup("F")
      .addCounter(new Counter("XXXXXX", 111111))
      .addCounter(new Counter("YYYYYY", 222222))
    );

    List<MSubmission> sources = new ArrayList<MSubmission>();
    MSubmission sourcex = new MSubmission();
    sourcex.setCounters(countersx);
    sources.add(sourcex);
    MSubmission sourcey = new MSubmission();
    sourcey.setCounters(countersy);
    sources.add(sourcey);

    List<MSubmission> targets = transfer(sources);
    assertNotNull(targets.get(0));
    target = targets.get(0).getCounters();
    group = target.getCounterGroup("C");
    assertNotNull(group);
    counter = group.getCounter("XXX");
    assertNotNull(counter);
    assertEquals(111, counter.getValue());
    counter = group.getCounter("YYY");
    assertNotNull(counter);
    assertEquals(222, counter.getValue());

    group = target.getCounterGroup("D");
    assertNotNull(group);
    counter = group.getCounter("XXXX");
    assertNotNull(counter);
    assertEquals(1111, counter.getValue());
    counter = group.getCounter("YYYY");
    assertNotNull(counter);
    assertEquals(2222, counter.getValue());

    assertNotNull(targets.get(1));
    target = targets.get(1).getCounters();
    group = target.getCounterGroup("E");
    assertNotNull(group);
    counter = group.getCounter("XXXXX");
    assertNotNull(counter);
    assertEquals(11111, counter.getValue());
    counter = group.getCounter("YYYYY");
    assertNotNull(counter);
    assertEquals(22222, counter.getValue());

    group = target.getCounterGroup("F");
    assertNotNull(group);
    counter = group.getCounter("XXXXXX");
    assertNotNull(counter);
    assertEquals(111111, counter.getValue());
    counter = group.getCounter("YYYYYY");
    assertNotNull(counter);
    assertEquals(222222, counter.getValue());
  }

  @Test
  public void testTransferFromSchema() {
    MSubmission source = new MSubmission();
    source.setFromSchema(getSchema());

    Schema target = transfer(source).getFromSchema();
    assertNotNull(target);
    assertEquals(getSchema(), target);
  }

  @Test
  public void testTransferToSchema() {
    MSubmission source = new MSubmission();
    source.setToSchema(getSchema());

    Schema target = transfer(source).getToSchema();
    assertNotNull(target);
    assertEquals(getSchema(), target);
  }

  private Schema getSchema() {
    return new Schema("schema")
      .addColumn(new Text("col1"))
      .addColumn(new Decimal("col2", 5, 2))
    ;
  }

  /**
   * Simulate transfer of MSubmission structure using SubmissionBean
   *
   * @param submission Submission to transfer
   * @return
   */
  private MSubmission transfer(MSubmission submission) {
    SubmissionsBean bean = new SubmissionsBean(submission);
    JSONObject json = bean.extract(false);

    String string = json.toString();

    JSONObject retrievedJson = JSONUtils.parse(string);
    SubmissionsBean retrievedBean = new SubmissionsBean();
    retrievedBean.restore(retrievedJson);

    return retrievedBean.getSubmissions().get(0);
  }

  /**
   * Simulate transfer of a list of MSubmission structures using SubmissionBean
   *
   * @param submissions Submissions to transfer
   * @return
   */
  private List<MSubmission> transfer(List<MSubmission> submissions) {
    SubmissionsBean bean = new SubmissionsBean(submissions);
    JSONObject json = bean.extract(false);

    String string = json.toString();

    JSONObject retrievedJson = JSONUtils.parse(string);
    SubmissionsBean retrievedBean = new SubmissionsBean();
    retrievedBean.restore(retrievedJson);

    return retrievedBean.getSubmissions();
  }
}

