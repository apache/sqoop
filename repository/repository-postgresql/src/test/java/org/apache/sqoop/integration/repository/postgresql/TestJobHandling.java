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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test job methods on Derby repository.
 */
@Test(groups = "postgresql")
public class TestJobHandling extends PostgresqlTestCase {

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
    handler.createJob(getJob(JOB_A_NAME, connectorA, connectorB, linkA, linkB), provider.getConnection());
    handler.createJob(getJob(JOB_B_NAME, connectorB, connectorA, linkB, linkA), provider.getConnection());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testFindJobFail() throws Exception {
    for (MJob job : handler.findJobs(provider.getConnection())) {
      handler.deleteJob(job.getPersistenceId(), provider.getConnection());
    }

    // Let's try to find non existing job
    handler.findJob(1, provider.getConnection());
  }

  @Test
  public void testFindJobSuccess() throws Exception {
    MJob firstJob = handler.findJob(1, provider.getConnection());
    assertNotNull(firstJob);
    assertEquals(1, firstJob.getPersistenceId());
    assertEquals(JOB_A_NAME, firstJob.getName());

    List<MConfig> configs;

    configs = firstJob.getFromJobConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    configs = firstJob.getToJobConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    configs = firstJob.getDriverConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testFindJobs() throws Exception {
    List<MJob> list;

    list = handler.findJobs(provider.getConnection());
    assertEquals(2, list.size());
    assertEquals(JOB_A_NAME, list.get(0).getName());
    assertEquals(JOB_B_NAME, list.get(1).getName());

    // Delete jobs
    for (MJob job : handler.findJobs(provider.getConnection())) {
      handler.deleteJob(job.getPersistenceId(), provider.getConnection());
    }

    // Load all two links on loaded repository
    list = handler.findJobs(provider.getConnection());
    assertEquals(0, list.size());
  }

  @Test
  public void testFindJobsByConnector() throws Exception {
    List<MJob> list = handler.findJobsForConnector(
        handler.findConnector("A", provider.getConnection()).getPersistenceId(),
        provider.getConnection());
    assertEquals(2, list.size());
    assertEquals(JOB_A_NAME, list.get(0).getName());
    assertEquals(JOB_B_NAME, list.get(1).getName());
  }

  @Test
  public void testFindJobsForNonExistingConnector() throws Exception {
    List<MJob> list = handler.findJobsForConnector(11, provider.getConnection());
    assertEquals(0, list.size());
  }

  @Test
  public void testExistsJob() throws Exception {
    assertTrue(handler.existsJob(1, provider.getConnection()));
    assertTrue(handler.existsJob(2, provider.getConnection()));
    assertFalse(handler.existsJob(3, provider.getConnection()));

    // Delete jobs
    for (MJob job : handler.findJobs(provider.getConnection())) {
      handler.deleteJob(job.getPersistenceId(), provider.getConnection());
    }

    // There shouldn't be anything on empty repository
    assertFalse(handler.existsJob(1, provider.getConnection()));
    assertFalse(handler.existsJob(2, provider.getConnection()));
    assertFalse(handler.existsJob(3, provider.getConnection()));
  }

  @Test
  public void testInUseJob() throws Exception {
    MSubmission submission = getSubmission(handler.findJob(1, provider.getConnection()), SubmissionStatus.RUNNING);
    handler.createSubmission(submission, provider.getConnection());

    assertTrue(handler.inUseJob(1, provider.getConnection()));
    assertFalse(handler.inUseJob(2, provider.getConnection()));
    assertFalse(handler.inUseJob(3, provider.getConnection()));
  }

  @Test
  public void testCreateJob() throws Exception {
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB")), 2);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB_INPUT")), 12);

    MJob retrieved = handler.findJob(1, provider.getConnection());
    assertEquals(1, retrieved.getPersistenceId());

    List<MConfig> configs;
    configs = retrieved.getFromJobConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    configs = retrieved.getToJobConfig().getConfigs();
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());

    configs = retrieved.getDriverConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testCreateDuplicateJob() throws Exception {
    // Duplicate jobs
    MJob job = handler.findJob(JOB_A_NAME, provider.getConnection());
    job.setPersistenceId(MJob.PERSISTANCE_ID_DEFAULT);
    handler.createJob(job, provider.getConnection());
  }

  @Test
  public void testUpdateJob() throws Exception {
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB")), 2);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB_INPUT")), 12);

    MJob job = handler.findJob(1, provider.getConnection());

    List<MConfig> configs;

    configs = job.getFromJobConfig().getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(null);

    configs = job.getToJobConfig().getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(null);

    configs = job.getDriverConfig().getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value
    ((MStringInput)configs.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(1).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value

    job.setName("name");

    handler.updateJob(job, provider.getConnection());

    assertEquals(1, job.getPersistenceId());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB")), 2);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB_INPUT")), 14);

    MJob retrieved = handler.findJob(1, provider.getConnection());
    assertEquals("name", retrieved.getName());

    configs = job.getFromJobConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    configs = job.getToJobConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());

    configs = retrieved.getDriverConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertNotNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals(((Map)configs.get(0).getInputs().get(1).getValue()).size(), 0);
  }

  @Test
  public void testEnableAndDisableJob() throws Exception {
    // disable job 1
    handler.enableJob(1, false, provider.getConnection());

    MJob retrieved = handler.findJob(1, provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(false, retrieved.getEnabled());

    // enable job 1
    handler.enableJob(1, true, provider.getConnection());

    retrieved = handler.findJob(1, provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(true, retrieved.getEnabled());
  }

  @Test
  public void testDeleteJob() throws Exception {
    handler.deleteJob(1, provider.getConnection());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB")), 1);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB_INPUT")), 6);

    handler.deleteJob(2, provider.getConnection());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB")), 0);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_JOB_INPUT")), 0);
  }
}