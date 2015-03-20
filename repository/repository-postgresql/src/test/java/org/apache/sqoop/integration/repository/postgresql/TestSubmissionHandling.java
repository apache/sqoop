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
package org.apache.sqoop.integration.repository.postgresql;

import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 *
 */
@Test(groups = "postgresql")
public class TestSubmissionHandling extends PostgresqlTestCase {
  public static final String CONNECTOR_A_NAME = "A";
  public static final String CONNECTOR_A_CLASSNAME = "org.apache.sqoop.test.A";
  public static final String CONNECTOR_A_VERSION = "1.0-test";
  public static final String CONNECTOR_B_NAME = "B";
  public static final String CONNECTOR_B_CLASSNAME = "org.apache.sqoop.test.B";
  public static final String CONNECTOR_B_VERSION = "1.0-test";
  public static final String LINK_A_NAME = "Link-A";
  public static final String LINK_B_NAME = "Link-B";
  public static final String JOB_A_NAME = "Job-A";
  public static final String JOB_B_NAME = "Job-B";

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    super.setUp();

    handler.registerDriver(getDriver(), provider.getConnection());
    MConnector connectorA = getConnector(CONNECTOR_A_NAME, CONNECTOR_A_CLASSNAME, CONNECTOR_A_VERSION, true, true);
    MConnector connectorB = getConnector(CONNECTOR_B_NAME, CONNECTOR_B_CLASSNAME, CONNECTOR_B_VERSION, true, true);
    handler.registerConnector(connectorA, provider.getConnection());
    handler.registerConnector(connectorB, provider.getConnection());
    MLink linkA = getLink(LINK_A_NAME, connectorA);
    MLink linkB = getLink(LINK_B_NAME, connectorB);
    handler.createLink(linkA, provider.getConnection());
    handler.createLink(linkB, provider.getConnection());
    MJob jobA = getJob(JOB_A_NAME, connectorA, connectorB, linkA, linkB);
    MJob jobB = getJob(JOB_B_NAME, connectorB, connectorA, linkB, linkA);
    handler.createJob(jobA, provider.getConnection());
    handler.createJob(jobB, provider.getConnection());
  }

  private void loadSubmissions() throws Exception {
    MJob jobA = handler.findJob(JOB_A_NAME, provider.getConnection());
    MJob jobB = handler.findJob(JOB_B_NAME, provider.getConnection());

    MSubmission submissionA = getSubmission(jobA, SubmissionStatus.RUNNING);
    submissionA.getCounters().getCounterGroup("test-1").addCounter(new Counter("counter-1"));
    submissionA.getCounters().getCounterGroup("test-1").addCounter(new Counter("counter-2"));
    submissionA.getCounters().getCounterGroup("test-1").getCounter("counter-1").setValue(300);
    MSubmission submissionB = getSubmission(jobA, SubmissionStatus.SUCCEEDED);
    MSubmission submissionC = getSubmission(jobB, SubmissionStatus.FAILED);
    MSubmission submissionD = getSubmission(jobB, SubmissionStatus.UNKNOWN);
    handler.createSubmission(submissionA, provider.getConnection());
    handler.createSubmission(submissionB, provider.getConnection());
    handler.createSubmission(submissionC, provider.getConnection());
    handler.createSubmission(submissionD, provider.getConnection());
  }

  @Test
  public void testFindSubmissionsUnfinished() throws Exception {
    List<MSubmission> submissions;

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());

    loadSubmissions();

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());
  }

  @Test
  public void testExistsSubmission() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsSubmission(1, provider.getConnection()));
    assertFalse(handler.existsSubmission(2, provider.getConnection()));
    assertFalse(handler.existsSubmission(3, provider.getConnection()));
    assertFalse(handler.existsSubmission(4, provider.getConnection()));
    assertFalse(handler.existsSubmission(5, provider.getConnection()));

    loadSubmissions();

    assertTrue(handler.existsSubmission(1, provider.getConnection()));
    assertTrue(handler.existsSubmission(2, provider.getConnection()));
    assertTrue(handler.existsSubmission(3, provider.getConnection()));
    assertTrue(handler.existsSubmission(4, provider.getConnection()));
    assertFalse(handler.existsSubmission(5, provider.getConnection()));
  }

  @Test
  public void testCreateSubmission() throws Exception {
    Date creationDate = new Date();
    Date updateDate = new Date();

    CounterGroup firstGroup = new CounterGroup("ga");
    CounterGroup secondGroup = new CounterGroup("gb");
    firstGroup.addCounter(new Counter("ca", 100));
    firstGroup.addCounter(new Counter("cb", 200));
    secondGroup.addCounter(new Counter("ca", 300));
    secondGroup.addCounter(new Counter("cd", 400));
    Counters counters = new Counters();
    counters.addCounterGroup(firstGroup);
    counters.addCounterGroup(secondGroup);

    MSubmission submission = new MSubmission();
    submission.setJobId(1);
    submission.setStatus(SubmissionStatus.RUNNING);
    submission.setCreationDate(creationDate);
    submission.setLastUpdateDate(updateDate);
    submission.setExternalJobId("job-x");
    submission.setExternalLink("http://somewhere");
    submission.getError().setErrorSummary("RuntimeException");
    submission.getError().setErrorDetails("Yeah it happens");
    submission.setCounters(counters);

    handler.createSubmission(submission, provider.getConnection());


    assertEquals(1, submission.getPersistenceId());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 1);

    List<MSubmission> submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());

    submission = submissions.get(0);

    assertEquals(1, submission.getJobId());
    assertEquals(SubmissionStatus.RUNNING, submission.getStatus());
    assertEquals(creationDate, submission.getCreationDate());
    assertEquals(updateDate, submission.getLastUpdateDate());
    assertEquals("job-x", submission.getExternalJobId());
    assertEquals("http://somewhere", submission.getExternalLink());
    assertEquals("RuntimeException", submission.getError().getErrorSummary());
    assertEquals("Yeah it happens", submission.getError().getErrorDetails());

    CounterGroup group;
    Counter counter;
    Counters retrievedCounters = submission.getCounters();
    assertNotNull(retrievedCounters);

    group = counters.getCounterGroup("ga");
    assertNotNull(group);

    counter = group.getCounter("ca");
    assertNotNull(counter);
    assertEquals(100, counter.getValue());

    counter = group.getCounter("cb");
    assertNotNull(counter);
    assertEquals(200, counter.getValue());

    group = counters.getCounterGroup("gb");
    assertNotNull(group);

    counter = group.getCounter("ca");
    assertNotNull(counter);
    assertEquals(300, counter.getValue());

    counter = group.getCounter("cd");
    assertNotNull(counter);
    assertEquals(400, counter.getValue());

    // Let's create second (simpler) connection
    submission = new MSubmission(1, new Date(), SubmissionStatus.SUCCEEDED, "job-x");
    handler.createSubmission(submission, provider.getConnection());

    assertEquals(2, submission.getPersistenceId());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 2);
  }

  @Test
  public void testUpdateSubmission() throws Exception {
    loadSubmissions();

    List<MSubmission> submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());

    MSubmission submission = submissions.get(0);
    submission.setStatus(SubmissionStatus.SUCCEEDED);

    handler.updateSubmission(submission, provider.getConnection());

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());
  }

  @Test
  public void testCreateSubmissionExceptionDetailsMoreThanMaxLimit() throws Exception {

    String externalLink = "http://somewheresomewheresomewheresomewheresomewheresomewheresomewheresomewheresomewheresomewheresomewheresom"
        + "ewheresomewheresomewheresomewheresomewher";

    String errorSummary = "RuntimeExceptionRuntimeExceptionRuntimeExceptionRuntimeExceptionRuntimeExceptionRuntimeExceptions"
        + "RuntimeExceptionRuntimeExceptionRuntimeExceptiontests";
    String errorDetail = "Yeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah"
        + " it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it hap"
        + "pensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYea"
        + "h it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it ha"
        + "ppensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah"
        + " it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happe"
        + "nsYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happens";
    MSubmission submission = new MSubmission();
    submission.setJobId(1);
    submission.setStatus(SubmissionStatus.RUNNING);
    submission.setCreationDate(new Date());
    submission.setLastUpdateDate(new Date());
    submission.setExternalJobId("job-x");
    submission.setExternalLink(externalLink + "more than 150");
    submission.getError().setErrorSummary("RuntimeException");
    submission.getError().setErrorDetails(errorDetail + "morethan750");
    submission.getError().setErrorSummary(errorSummary + "morethan150");

    handler.createSubmission(submission, provider.getConnection());
    List<MSubmission> submissions = handler.findSubmissionsForJob(1, provider.getConnection());
    assertNotNull(submissions);

    assertEquals(errorDetail, submissions.get(0).getError().getErrorDetails());
    assertEquals(errorSummary, submissions.get(0).getError().getErrorSummary());
    assertEquals(externalLink, submissions.get(0).getExternalLink());

  }

  @Test
  public void testUpdateSubmissionExceptionDetailsMoreThanMaxLimit() throws Exception {
    loadSubmissions();

    List<MSubmission> submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());

    String errorSummary = "RuntimeExceptionRuntimeExceptionRuntimeExceptionRuntimeExceptionRuntimeExceptionRuntimeExceptions"
        + "RuntimeExceptionRuntimeExceptionRuntimeExceptiontests";

    String errorDetail = "Yeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah"
        + " it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it hap"
        + "pensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYea"
        + "h it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it ha"
        + "ppensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah"
        + " it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happe"
        + "nsYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happensYeah it happens";
    MSubmission submission = submissions.get(0);
    String externalLink = submission.getExternalLink();
    submission.getError().setErrorDetails(errorDetail + "morethan750");
    submission.getError().setErrorSummary(errorSummary + "morethan150");
    submission.setExternalLink("cantupdate");

    handler.updateSubmission(submission, provider.getConnection());

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());

    assertNotNull(submissions);
    assertEquals(errorDetail, submissions.get(0).getError().getErrorDetails());
    assertEquals(errorSummary, submissions.get(0).getError().getErrorSummary());
    // note we dont allow external link update
    assertEquals(externalLink, submissions.get(0).getExternalLink());

  }

  @Test
  public void testPurgeSubmissions() throws Exception {
    loadSubmissions();
    List<MSubmission> submissions;

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 4);

    Calendar calendar = Calendar.getInstance();
    // 2012-01-03 05:05:05
    calendar.set(2012, Calendar.JANUARY, 3, 5, 5, 5);
    handler.purgeSubmissions(calendar.getTime(), provider.getConnection());

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 4);

    handler.purgeSubmissions(new Date(), provider.getConnection());

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 0);

    handler.purgeSubmissions(new Date(), provider.getConnection());

    submissions = handler.findUnfinishedSubmissions(provider.getConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 0);
  }

  /**
   * Test that by directly removing jobs we will also remove associated
   * submissions and counters.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteJobs() throws Exception {
    MJob jobA = handler.findJob(JOB_A_NAME, provider.getConnection());
    MJob jobB = handler.findJob(JOB_B_NAME, provider.getConnection());

    loadSubmissions();
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 4);

    handler.deleteJob(jobA.getPersistenceId(), provider.getConnection());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 2);

    handler.deleteJob(jobB.getPersistenceId(), provider.getConnection());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_SUBMISSION")), 0);
  }
}
