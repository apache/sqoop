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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.junit.Before;
import org.junit.Test;

/**
 * Test job methods on Derby repository.
 */
public class TestJobHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;
  Connection derbyConnection;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    derbyConnection = getDerbyDatabaseConnection();
    handler = new DerbyRepositoryHandler();
    // We always needs create/ upgrade schema for this test case
    createOrUpgradeSchemaForLatestVersion();
    loadConnectorAndDriverConfig();
    loadLinksForLatestVersion();
  }

  @Test
  public void testFindJob() throws Exception {
    // Let's try to find non existing job
    try {
      handler.findJob(1, derbyConnection);
      fail();
    } catch(SqoopException ex) {
      assertEquals(DerbyRepoError.DERBYREPO_0030, ex.getErrorCode());
    }

    loadJobsForLatestVersion();

    MJob firstJob = handler.findJob(1, derbyConnection);
    assertNotNull(firstJob);
    assertEquals(1, firstJob.getPersistenceId());
    assertEquals("JA0", firstJob.getName());

    List<MConfig> configs;

    configs = firstJob.getJobConfig(Direction.FROM).getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Value5", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value5", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    configs = firstJob.getJobConfig(Direction.TO).getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Value9", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value9", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    configs = firstJob.getDriverConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Value13", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value15", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testFindJobs() throws Exception {
    List<MJob> list;
    // Load empty list on empty repository
    list = handler.findJobs(derbyConnection);
    assertEquals(0, list.size());
    loadJobsForLatestVersion();

    // Load all two connections on loaded repository
    list = handler.findJobs(derbyConnection);
    assertEquals(4, list.size());

    assertEquals("JA0", list.get(0).getName());

    assertEquals("JB0", list.get(1).getName());

    assertEquals("JC0", list.get(2).getName());

    assertEquals("JD0", list.get(3).getName());
  }

  @Test
  public void testExistsJob() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsJob(1, derbyConnection));
    assertFalse(handler.existsJob(2, derbyConnection));
    assertFalse(handler.existsJob(3, derbyConnection));
    assertFalse(handler.existsJob(4, derbyConnection));
    assertFalse(handler.existsJob(5, derbyConnection));

    loadJobsForLatestVersion();

    assertTrue(handler.existsJob(1, derbyConnection));
    assertTrue(handler.existsJob(2, derbyConnection));
    assertTrue(handler.existsJob(3, derbyConnection));
    assertTrue(handler.existsJob(4, derbyConnection));
    assertFalse(handler.existsJob(5, derbyConnection));
  }

  @Test
  public void testInUseJob() throws Exception {
    loadJobsForLatestVersion();
    loadSubmissions();

    assertTrue(handler.inUseJob(1, derbyConnection));
    assertFalse(handler.inUseJob(2, derbyConnection));
    assertFalse(handler.inUseJob(3, derbyConnection));
    assertFalse(handler.inUseJob(4, derbyConnection));
  }

  @Test
  public void testCreateJob() throws Exception {
    MJob job = getJob();

    // Load some data
    fillJob(job);

    handler.createJob(job, derbyConnection);

    assertEquals(1, job.getPersistenceId());
    assertCountForTable("SQOOP.SQ_JOB", 1);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 6);

    MJob retrieved = handler.findJob(1, derbyConnection);
    assertEquals(1, retrieved.getPersistenceId());

    List<MConfig> configs;
    configs = job.getJobConfig(Direction.FROM).getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    configs = job.getJobConfig(Direction.TO).getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());

    configs = job.getDriverConfig().getConfigs();
    assertEquals("Value13", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value15", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    // Let's create second job
    job = getJob();
    fillJob(job);

    handler.createJob(job, derbyConnection);

    assertEquals(2, job.getPersistenceId());
    assertCountForTable("SQOOP.SQ_JOB", 2);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 12);
  }

  @Test(expected=SqoopException.class)
  public void testCreateDuplicateJob() throws Exception {
    // Duplicate jobs
    MJob job = getJob();
    fillJob(job);
    job.setName("test");
    handler.createJob(job, getDerbyDatabaseConnection());
    assertEquals(1, job.getPersistenceId());

    job.setPersistenceId(MJob.PERSISTANCE_ID_DEFAULT);
    handler.createJob(job, getDerbyDatabaseConnection());
  }

  @Test
  public void testUpdateJob() throws Exception {
    loadJobsForLatestVersion();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);

    MJob job = handler.findJob(1, derbyConnection);

    List<MConfig> configs;

    configs = job.getJobConfig(Direction.FROM).getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(null);

    configs = job.getJobConfig(Direction.TO).getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(null);

    configs = job.getDriverConfig().getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value
    ((MStringInput)configs.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(1).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value

    job.setName("name");

    handler.updateJob(job, derbyConnection);

    assertEquals(1, job.getPersistenceId());
    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 26);

    MJob retrieved = handler.findJob(1, derbyConnection);
    assertEquals("name", retrieved.getName());

    configs = job.getJobConfig(Direction.FROM).getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    configs = job.getJobConfig(Direction.TO).getConfigs();
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
    loadJobsForLatestVersion();

    // disable job 1
    handler.enableJob(1, false, derbyConnection);

    MJob retrieved = handler.findJob(1, derbyConnection);
    assertNotNull(retrieved);
    assertEquals(false, retrieved.getEnabled());

    // enable job 1
    handler.enableJob(1, true, derbyConnection);

    retrieved = handler.findJob(1, derbyConnection);
    assertNotNull(retrieved);
    assertEquals(true, retrieved.getEnabled());
  }

  @Test
  public void testDeleteJob() throws Exception {
    loadJobsForLatestVersion();

    handler.deleteJob(1, derbyConnection);
    assertCountForTable("SQOOP.SQ_JOB", 3);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 18);

    handler.deleteJob(2, derbyConnection);
    assertCountForTable("SQOOP.SQ_JOB", 2);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 12);

    handler.deleteJob(3, derbyConnection);
    assertCountForTable("SQOOP.SQ_JOB", 1);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 6);

    handler.deleteJob(4, derbyConnection);
    assertCountForTable("SQOOP.SQ_JOB", 0);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 0);
  }

  public MJob getJob() {
    return new MJob(1, 1, 1, 1,
      handler.findConnector("A", derbyConnection).getFromConfig(),
      handler.findConnector("A", derbyConnection).getToConfig(),
      handler.findDriver(derbyConnection).getDriverConfig()
    );
  }
}