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
package org.apache.sqoop.repository.derby;

import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.testng.Assert.*;

/**
 *
 */
public class TestSubmissionHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();
    // We always needs schema for this test case
    super.createOrUpgradeSchemaForLatestVersion();

    // We always needs connector and framework structures in place
    loadConnectorAndDriverConfig();

    // We also always needs connection metadata in place
    loadLinksForLatestVersion();

    // And finally we always needs job metadata in place
    loadJobsForLatestVersion();
  }

  @Test
  public void testFindSubmissionsUnfinished() throws Exception {
    List<MSubmission> submissions;

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());

    loadSubmissions();

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(2, submissions.size());
  }

  @Test
  public void testExistsSubmission() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsSubmission(1, getDerbyDatabaseConnection()));
    assertFalse(handler.existsSubmission(2, getDerbyDatabaseConnection()));
    assertFalse(handler.existsSubmission(3, getDerbyDatabaseConnection()));
    assertFalse(handler.existsSubmission(4, getDerbyDatabaseConnection()));
    assertFalse(handler.existsSubmission(5, getDerbyDatabaseConnection()));
    assertFalse(handler.existsSubmission(6, getDerbyDatabaseConnection()));

    loadSubmissions();

    assertTrue(handler.existsSubmission(1, getDerbyDatabaseConnection()));
    assertTrue(handler.existsSubmission(2, getDerbyDatabaseConnection()));
    assertTrue(handler.existsSubmission(3, getDerbyDatabaseConnection()));
    assertTrue(handler.existsSubmission(4, getDerbyDatabaseConnection()));
    assertTrue(handler.existsSubmission(5, getDerbyDatabaseConnection()));
    assertFalse(handler.existsSubmission(6, getDerbyDatabaseConnection()));
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

    handler.createSubmission(submission, getDerbyDatabaseConnection());

    assertEquals(1, submission.getPersistenceId());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 1);

    List<MSubmission> submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());

    submission = submissions.get(0);

    assertEquals(1, submission.getJobId());
    assertEquals(SubmissionStatus.RUNNING, submission.getStatus());
    assertEquals(creationDate.getTime(), submission.getCreationDate().getTime());
    assertEquals(updateDate.getTime(), submission.getLastUpdateDate().getTime());
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
    handler.createSubmission(submission, getDerbyDatabaseConnection());

    assertEquals(2, submission.getPersistenceId());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 2);
  }

  @Test
  public void testUpdateSubmission() throws Exception {
    loadSubmissions();

    List<MSubmission> submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(2, submissions.size());

    MSubmission submission = submissions.get(0);
    submission.setStatus(SubmissionStatus.SUCCEEDED);

    handler.updateSubmission(submission, getDerbyDatabaseConnection());

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());
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

    handler.createSubmission(submission, getDerbyDatabaseConnection());
    List<MSubmission> submissions = handler.findSubmissionsForJob(1, getDerbyDatabaseConnection());
    assertNotNull(submissions);

    assertEquals(errorDetail, submissions.get(0).getError().getErrorDetails());
    assertEquals(errorSummary, submissions.get(0).getError().getErrorSummary());
    assertEquals(externalLink, submissions.get(0).getExternalLink());

  }

  @Test
  public void testUpdateSubmissionExceptionDetailsMoreThanMaxLimit() throws Exception {
    loadSubmissions();

    List<MSubmission> submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(2, submissions.size());

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

    handler.updateSubmission(submission, getDerbyDatabaseConnection());

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());

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

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(2, submissions.size());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 5);

    Calendar calendar = Calendar.getInstance();
    // 2012-01-03 05:05:05
    calendar.set(2012, Calendar.JANUARY, 3, 5, 5, 5);
    handler.purgeSubmissions(calendar.getTime(), getDerbyDatabaseConnection());

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 2);

    handler.purgeSubmissions(new Date(), getDerbyDatabaseConnection());

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 0);

    handler.purgeSubmissions(new Date(), getDerbyDatabaseConnection());

    submissions = handler.findUnfinishedSubmissions(getDerbyDatabaseConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 0);
  }

  /**
   * Test that by directly removing jobs we will also remove associated
   * submissions and counters.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteJobs() throws Exception {
    loadSubmissions();
    assertCountForTable("SQOOP.SQ_SUBMISSION", 5);

    handler.deleteJob(1, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 3);

    handler.deleteJob(2, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 2);

    handler.deleteJob(3, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 1);

    handler.deleteJob(4, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 0);
  }
}
