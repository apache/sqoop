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

import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 */
public class TestSubmissionHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();

    // We always needs schema for this test case
    createSchema();

    // We always needs connector and framework structures in place
    loadConnectorAndFramework();

    // We also always needs connection metadata in place
    loadConnections();

    // And finally we always needs job metadata in place
    loadJobs();
  }

  public void testFindSubmissionsUnfinished() throws Exception {
    List<MSubmission> submissions;

    submissions = handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());

    loadSubmissions();

    submissions = handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(2, submissions.size());
  }

  public void testExistsSubmission() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsSubmission(1, getDerbyConnection()));
    assertFalse(handler.existsSubmission(2, getDerbyConnection()));
    assertFalse(handler.existsSubmission(3, getDerbyConnection()));
    assertFalse(handler.existsSubmission(4, getDerbyConnection()));
    assertFalse(handler.existsSubmission(5, getDerbyConnection()));
    assertFalse(handler.existsSubmission(6, getDerbyConnection()));

    loadSubmissions();

    assertTrue(handler.existsSubmission(1, getDerbyConnection()));
    assertTrue(handler.existsSubmission(2, getDerbyConnection()));
    assertTrue(handler.existsSubmission(3, getDerbyConnection()));
    assertTrue(handler.existsSubmission(4, getDerbyConnection()));
    assertTrue(handler.existsSubmission(5, getDerbyConnection()));
    assertFalse(handler.existsSubmission(6, getDerbyConnection()));
  }

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
    submission.setExternalId("job-x");
    submission.setExternalLink("http://somewhere");
    submission.setExceptionInfo("RuntimeException");
    submission.setExceptionStackTrace("Yeah it happens");
    submission.setCounters(counters);

    handler.createSubmission(submission, getDerbyConnection());

    assertEquals(1, submission.getPersistenceId());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 1);

    List<MSubmission> submissions =
      handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());

    submission = submissions.get(0);

    assertEquals(1, submission.getJobId());
    assertEquals(SubmissionStatus.RUNNING, submission.getStatus());
    assertEquals(creationDate, submission.getCreationDate());
    assertEquals(updateDate, submission.getLastUpdateDate());
    assertEquals("job-x", submission.getExternalId());
    assertEquals("http://somewhere", submission.getExternalLink());
    assertEquals("RuntimeException", submission.getExceptionInfo());
    assertEquals("Yeah it happens", submission.getExceptionStackTrace());

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
    submission =
      new MSubmission(1, new Date(), SubmissionStatus.SUCCEEDED, "job-x");
    handler.createSubmission(submission, getDerbyConnection());

    assertEquals(2, submission.getPersistenceId());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 2);
  }

  public void testUpdateConnection() throws Exception {
    loadSubmissions();

    List<MSubmission> submissions =
      handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(2, submissions.size());

    MSubmission submission = submissions.get(0);
    submission.setStatus(SubmissionStatus.SUCCEEDED);

    handler.updateSubmission(submission, getDerbyConnection());

    submissions = handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());
  }

  public void testPurgeSubmissions() throws Exception {
    loadSubmissions();
    List<MSubmission> submissions;

    submissions = handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(2, submissions.size());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 5);

    Calendar calendar = Calendar.getInstance();
    // 2012-01-03 05:05:05
    calendar.set(2012, Calendar.JANUARY, 3, 5, 5, 5);
    handler.purgeSubmissions(calendar.getTime(), getDerbyConnection());

    submissions = handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(1, submissions.size());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 2);

    handler.purgeSubmissions(new Date(), getDerbyConnection());

    submissions = handler.findSubmissionsUnfinished(getDerbyConnection());
    assertNotNull(submissions);
    assertEquals(0, submissions.size());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 0);

    handler.purgeSubmissions(new Date(), getDerbyConnection());

    submissions = handler.findSubmissionsUnfinished(getDerbyConnection());
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
  public void testDeleteJobs() throws Exception {
    loadSubmissions();
    assertCountForTable("SQOOP.SQ_SUBMISSION", 5);

    handler.deleteJob(1, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 3);

    handler.deleteJob(2, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 2);

    handler.deleteJob(3, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 1);

    handler.deleteJob(4, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_SUBMISSION", 0);
  }
}
