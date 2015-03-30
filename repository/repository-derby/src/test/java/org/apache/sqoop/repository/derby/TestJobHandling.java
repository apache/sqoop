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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigUpdateEntityType;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.error.code.CommonRepositoryError;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test job methods on Derby repository.
 */
public class TestJobHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;
  Connection derbyConnection;

  @BeforeMethod(alwaysRun = true)
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
      assertEquals(CommonRepositoryError.COMMON_0027, ex.getErrorCode());
    }

    loadJobsForLatestVersion();

    MJob firstJob = handler.findJob(1, derbyConnection);
    assertNotNull(firstJob);
    assertEquals(1, firstJob.getPersistenceId());
    assertEquals("JA0", firstJob.getName());

    List<MConfig> configs;

    configs = firstJob.getFromJobConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Value5", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value5", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    configs = firstJob.getToJobConfig().getConfigs();
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

    // Load all two links on loaded repository
    list = handler.findJobs(derbyConnection);
    assertEquals(4, list.size());

    assertEquals("JA0", list.get(0).getName());
    assertEquals("JB0", list.get(1).getName());
    assertEquals("JC0", list.get(2).getName());
    assertEquals("JD0", list.get(3).getName());
  }

  @Test
  public void testFindJobsByConnector() throws Exception {
    List<MJob> list;
    // Load empty list on empty repository
    list = handler.findJobs(derbyConnection);
    assertEquals(0, list.size());
    loadJobsForLatestVersion();

    // Load all 4 jobs on loaded repository
    list = handler.findJobsForConnector(1, derbyConnection);
    assertEquals(4, list.size());

    assertEquals("JA0", list.get(0).getName());
    assertEquals("JB0", list.get(1).getName());
    assertEquals("JC0", list.get(2).getName());
    assertEquals("JD0", list.get(3).getName());
  }

  @Test
  public void testFindJobsForNonExistingConnector() throws Exception {
    List<MJob> list;
    // Load empty list on empty repository
    list = handler.findJobs(derbyConnection);
    assertEquals(0, list.size());
    loadJobsForLatestVersion();

    list = handler.findJobsForConnector(11, derbyConnection);
    assertEquals(0, list.size());
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
    configs = job.getFromJobConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    configs = job.getToJobConfig().getConfigs();
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

  @Test(expectedExceptions=SqoopException.class)
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

    configs = job.getFromJobConfig().getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    Map<String, String> newFromMap = new HashMap<String, String>();
    newFromMap.put("1F", "foo");
    newFromMap.put("2F", "bar");

    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(newFromMap);

    configs = job.getToJobConfig().getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    Map<String, String> newToMap = new HashMap<String, String>();
    newToMap.put("1T", "foo");
    newToMap.put("2T", "bar");

    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(newToMap);

    configs = job.getDriverConfig().getConfigs();
    ((MStringInput)configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(0).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value
    ((MStringInput)configs.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)configs.get(1).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value

    job.setName("name");

    handler.updateJob(job, derbyConnection);

    assertEquals(1, job.getPersistenceId());
    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 28);

    MJob retrieved = handler.findJob(1, derbyConnection);
    assertEquals("name", retrieved.getName());

    configs = job.getFromJobConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertEquals(newFromMap, configs.get(0).getInputs().get(1).getValue());
    configs = job.getToJobConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertEquals(newToMap, configs.get(0).getInputs().get(1).getValue());

    configs = retrieved.getDriverConfig().getConfigs();
    assertEquals(2, configs.size());
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertNotNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals(((Map) configs.get(0).getInputs().get(1).getValue()).size(), 0);
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

  @Test
  public void testUpdateJobConfig() throws Exception {
    loadJobsForLatestVersion();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);
    MJob job = handler.findJob(1, derbyConnection);

    List<MConfig> fromConfigs = job.getFromJobConfig().getConfigs();
    MConfig fromConfig = fromConfigs.get(0).clone(false);
    MConfig newFromConfig = new MConfig(fromConfig.getName(), fromConfig.getInputs());

    ((MStringInput) newFromConfig.getInputs().get(0)).setValue("FromJobConfigUpdated");

    handler.updateJobConfig(job.getPersistenceId(), newFromConfig, MConfigUpdateEntityType.USER,
        derbyConnection);

    MJob updatedJob = handler.findJob(1, derbyConnection);
    MFromConfig newFromConfigs = updatedJob.getFromJobConfig();
    assertEquals(2, newFromConfigs.getConfigs().size());
    MConfig updatedFromConfig = newFromConfigs.getConfigs().get(0);
    assertEquals("FromJobConfigUpdated", updatedFromConfig.getInputs().get(0).getValue());

    List<MConfig> toConfigs = job.getToJobConfig().getConfigs();
    MConfig toConfig = toConfigs.get(0).clone(false);
    MConfig newToConfig = new MConfig(toConfig.getName(), toConfig.getInputs());

    ((MStringInput) newToConfig.getInputs().get(0)).setValue("ToJobConfigUpdated");

    handler.updateJobConfig(job.getPersistenceId(), newToConfig, MConfigUpdateEntityType.USER,
        derbyConnection);

    updatedJob = handler.findJob(1, derbyConnection);
    MToConfig newToConfigs = updatedJob.getToJobConfig();
    assertEquals(2, newToConfigs.getConfigs().size());
    MConfig updatedToConfig = newToConfigs.getConfigs().get(0);
    assertEquals("ToJobConfigUpdated", updatedToConfig.getInputs().get(0).getValue());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testIncorrectEntityCausingConfigUpdate() throws Exception {
    loadJobsForLatestVersion();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);
    MJob job = handler.findJob(1, derbyConnection);

    List<MConfig> fromConfigs = job.getFromJobConfig().getConfigs();
    MConfig fromConfig = fromConfigs.get(0).clone(false);
    MConfig newFromConfig = new MConfig(fromConfig.getName(), fromConfig.getInputs());
    HashMap<String, String> newMap = new HashMap<String, String>();
    newMap.put("1", "foo");
    newMap.put("2", "bar");

    ((MMapInput) newFromConfig.getInputs().get(1)).setValue(newMap);

    handler.updateJobConfig(job.getPersistenceId(), newFromConfig, MConfigUpdateEntityType.USER,
        derbyConnection);
  }

  @Test
  public void testFindAndUpdateJobConfig() throws Exception {
    loadJobsForLatestVersion();
    MJob job = handler.findJob(1, derbyConnection);

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);
    MConfig fromConfig = handler.findFromJobConfig(1, "C1JOB1", derbyConnection);
    assertEquals("Value5", fromConfig.getInputs().get(0).getValue());
    assertNull(fromConfig.getInputs().get(1).getValue());

    MConfig toConfig = handler.findToJobConfig(1, "C2JOB2", derbyConnection);
    assertEquals("Value11", toConfig.getInputs().get(0).getValue());
    assertNull(toConfig.getInputs().get(1).getValue());
    HashMap<String, String> newMap = new HashMap<String, String>();
    newMap.put("1UPDATED", "foo");
    newMap.put("2UPDATED", "bar");
    ((MStringInput) toConfig.getInputs().get(0)).setValue("test");
    ((MMapInput) toConfig.getInputs().get(1)).setValue(newMap);

    handler.updateJobConfig(job.getPersistenceId(), toConfig, MConfigUpdateEntityType.USER,
        derbyConnection);
    assertEquals("test", toConfig.getInputs().get(0).getValue());
    assertEquals(newMap, toConfig.getInputs().get(1).getValue());

    MConfig driverConfig = handler.findDriverJobConfig(1, "d1", derbyConnection);
    assertEquals("Value13", driverConfig.getInputs().get(0).getValue());
    assertNull(driverConfig.getInputs().get(1).getValue());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNonExistingFromConfigFetch() throws Exception {
    loadJobsForLatestVersion();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);
    handler.findFromJobConfig(1, "Non-ExistingC1JOB1", derbyConnection);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNonExistingToConfigFetch() throws Exception {
    loadJobsForLatestVersion();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);
    handler.findToJobConfig(1, "Non-ExistingC2JOB1", derbyConnection);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNonExistingDriverConfigFetch() throws Exception {
    loadJobsForLatestVersion();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);
    handler.findDriverJobConfig(1, "Non-Existingd1", derbyConnection);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNonExistingJobConfig() throws Exception {
    loadJobsForLatestVersion();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);
    // 11 does not exist
    handler.findDriverJobConfig(11, "Non-d1", derbyConnection);
  }

  public MJob getJob() {
    return new MJob(1, 1, 1, 1, handler.findConnector("A", derbyConnection).getFromConfig(),
        handler.findConnector("A", derbyConnection).getToConfig(), handler.findDriver(
            MDriver.DRIVER_NAME, derbyConnection).getDriverConfig());
  }
}