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

package org.apache.sqoop.metastore;

import static org.apache.sqoop.metastore.GenericJobStorage.META_CONNECT_KEY;
import static org.apache.sqoop.metastore.GenericJobStorage.META_PASSWORD_KEY;
import static org.apache.sqoop.metastore.GenericJobStorage.META_USERNAME_KEY;

import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testcategories.sqooptest.IntegrationTest;
import org.apache.sqoop.tool.VersionTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.DefaultManagerFactory;
import org.apache.sqoop.tool.ImportTool;
import org.apache.sqoop.metastore.JobData;
import org.apache.sqoop.metastore.JobStorage;
import org.apache.sqoop.metastore.JobStorageFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Test the metastore and job-handling features,
 * implemented for specific database services in sub-classes.
 */
@Category(IntegrationTest.class)
public abstract class SavedJobsTestBase {

  public static final String TEST_JOB = "testJob";
  public static final String TEST_TABLE_NAME = "abcd";
  public static final String TEST_TABLE_NAME_2 = "efgh";
  public static final String TEST_JOB_2 = "testJob2";
  public static final String TEST_JOB_3 = "testJob3";
  public static final String TEST_TABLE_NAME_3 = "ijkl";
  private String metaConnect;
  private String metaUser;
  private String metaPassword;
  private String driverClass;
  private JobStorage storage;

  private Configuration conf;
  private Map<String, String> descriptor;

  public SavedJobsTestBase(String metaConnect, String metaUser, String metaPassword, String driverClass){
    this.metaConnect = metaConnect;
    this.metaUser = metaUser;
    this.metaPassword = metaPassword;
    this.driverClass = driverClass;
    this.descriptor = new TreeMap<>();
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    // Delete db state between tests.
    resetJobSchema();
    conf = newConf();

    descriptor.put(META_CONNECT_KEY, metaConnect);
    descriptor.put(META_USERNAME_KEY, metaUser);
    descriptor.put(META_PASSWORD_KEY, metaPassword);

    JobStorageFactory ssf = new JobStorageFactory(conf);
    storage = ssf.getJobStorage(descriptor);
    storage.open(descriptor);
  }

  @After
  public void tearDown() throws Exception {
    descriptor.clear();
    storage.close();
  }

  public void resetJobSchema()
          throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(metaConnect);
    options.setUsername(metaUser);
    options.setPassword(metaPassword);
    options.setDriverClassName(driverClass);

    resetSchema(options);
  }

  /**
   * Drop all tables in the configured HSQLDB-based schema/user/pass.
   */
  public static void resetSchema(SqoopOptions options) throws SQLException {
    JobData jd = new JobData();
    jd.setSqoopOptions(options);
    DefaultManagerFactory dmf = new DefaultManagerFactory();
    ConnManager manager = dmf.accept(jd);
    Connection c = manager.getConnection();
    Statement s = c.createStatement();
    try {
      String [] tables = manager.listTables();
      for (String table : tables) {
        if(table.equals("SQOOP_ROOT") || table.equals("SQOOP_SESSIONS")){
          s.execute("DROP TABLE " + manager.escapeTableName(table));
        }
      }

      c.commit();
    } finally {
      s.close();
    }
  }

  public Configuration newConf() {
    Configuration conf = new Configuration();
    conf.set(META_CONNECT_KEY, metaConnect);
    conf.set(META_USERNAME_KEY, metaUser);
    conf.set(META_PASSWORD_KEY, metaPassword);

    return conf;
  }

  @Test
  public void testReadJobDoesExistPasses() throws Exception{
    storage.create(TEST_JOB, createTestJobData(TEST_TABLE_NAME));

    assertEquals("Read did not return job data correctly",
            storage.read(TEST_JOB).getSqoopOptions().getTableName(),
            TEST_TABLE_NAME);
  }

  @Test
  public void testUpdateJob() throws  Exception {
    storage.create(TEST_JOB, createTestJobData(TEST_TABLE_NAME));

    storage.update(TEST_JOB, createTestJobData(TEST_TABLE_NAME_2) );

    assertEquals("Update did not change data correctly",
            storage.read(TEST_JOB).getSqoopOptions().getTableName(),
            TEST_TABLE_NAME_2);
  }

  @Test
  public void testList() throws IOException {
    storage.create(TEST_JOB, createTestJobData(TEST_TABLE_NAME));
    storage.create(TEST_JOB_2, createTestJobData(TEST_TABLE_NAME_2));
    storage.create(TEST_JOB_3, createTestJobData(TEST_TABLE_NAME_3));

    assertThat("List did not return correct job data",
            storage.list(), hasItems(TEST_JOB, TEST_JOB_2, TEST_JOB_3));
  }

  @Test
  public void testCreateSameJob() throws IOException {

    // Job list should start out empty.
    List<String> jobs = storage.list();
    assertEquals("Job list should start out empty", 0, jobs.size());

    // Create a job that displays the version.
    JobData data = new JobData(new SqoopOptions(), new VersionTool());
    storage.create(TEST_JOB, data);

    jobs = storage.list();
    assertEquals("Test Job not created correctly",1, jobs.size());
    assertEquals("Test Job data not returned correctly", TEST_JOB, jobs.get(0));

    try {
      // Try to create that same job name again. This should fail.
      thrown.expect(IOException.class);
      thrown.reportMissingExceptionWithMessage("Expected IOException since job already exists");
      storage.create(TEST_JOB, data);
    } finally {
      jobs = storage.list();
      assertEquals("Incorrect number of jobs present",1, jobs.size());

      // Restore our job, check that it exists.
      JobData outData = storage.read(TEST_JOB);
      assertEquals("Test job does not exist", new VersionTool().getToolName(),
          outData.getSqoopTool().getToolName());
    }
  }

  @Test
  public void testDeleteJob() throws IOException {
    // Job list should start out empty.
    List<String> jobs = storage.list();
    assertEquals("Job List should start out empty", 0, jobs.size());

    // Create a job that displays the version.
    JobData data = new JobData(new SqoopOptions(), new VersionTool());
    storage.create(TEST_JOB, data);

    jobs = storage.list();
    assertEquals("Incorrect number of jobs present",1, jobs.size());
    assertEquals("Test Job created incorrectly", TEST_JOB, jobs.get(0));

    // Now delete the job.
    storage.delete(TEST_JOB);

    // After delete, we should have no jobs.
    jobs = storage.list();
    assertEquals("Job was not deleted correctly", 0, jobs.size());
  }

  @Test
  public void testRestoreNonExistingJob() throws IOException {
      // Try to restore a job that doesn't exist. Watch it fail.
      thrown.expect(IOException.class);
      thrown.reportMissingExceptionWithMessage("Expected IOException since job doesn't exist");
      storage.read("DoesNotExist");
  }

  @Test
  public void testCreateJobWithExtraArgs() throws IOException {

        // Job list should start out empty.
        List<String> jobs = storage.list();
        assertEquals("Job list should start out empty", 0, jobs.size());

        // Create a job with extra args
        SqoopOptions opts = new SqoopOptions();
        String[] args = {"-schema", "test"};
        opts.setExtraArgs(args);
        JobData data = new JobData(opts, new VersionTool());
        storage.create(TEST_JOB, data);

        jobs = storage.list();
        assertEquals("Incorrect number of jobs", 1, jobs.size());
        assertEquals("Job not created properly", TEST_JOB, jobs.get(0));

        // Restore our job, check that it exists.
        JobData outData = storage.read(TEST_JOB);
        assertEquals("Incorrect Tool in Test Job",
                new VersionTool().getToolName(),
                outData.getSqoopTool().getToolName());

        String[] storedArgs = outData.getSqoopOptions().getExtraArgs();
        for(int index = 0; index < args.length; ++index) {
            assertEquals(args[index], storedArgs[index]);
        }

        // Now delete the job.
        storage.delete(TEST_JOB);
    }

  @Test
  public void testMultiConnections() throws IOException {

    // Job list should start out empty.
    List<String> jobs = storage.list();
    assertEquals("Job list should start out empty", 0, jobs.size());

    // Create a job that displays the version.
    JobData data = new JobData(new SqoopOptions(), new VersionTool());
    storage.create(TEST_JOB, data);

    jobs = storage.list();
    assertEquals("Incorrect number of jobs", 1, jobs.size());
    assertEquals("Job not created correctly", TEST_JOB, jobs.get(0));

    storage.close(); // Close the existing connection

    // Now re-open the storage.
    storage.open(descriptor);

    jobs = storage.list();
    assertEquals("Test Job did not persist through re-open", 1, jobs.size());
    assertEquals("Job data not correct after re-open", TEST_JOB, jobs.get(0));

    // Restore our job, check that it exists.
    JobData outData = storage.read(TEST_JOB);
    assertEquals("Incorrect Tool in Test Job",
            new VersionTool().getToolName(),
            outData.getSqoopTool().getToolName());
  }

  private org.apache.sqoop.metastore.JobData createTestJobData(String setTableName) throws IOException {
    SqoopOptions testOpts = new SqoopOptions();
    testOpts.setTableName(setTableName);
    ImportTool testTool = new ImportTool();
    return new org.apache.sqoop.metastore.JobData(testOpts,testTool);

  }
}