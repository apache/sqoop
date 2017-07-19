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

package com.cloudera.sqoop.metastore;

import java.sql.SQLException;
import java.sql.Statement;

import java.util.*;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.MySQLTestUtils;
import com.cloudera.sqoop.manager.OracleUtils;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.VersionTool;
import org.apache.sqoop.manager.*;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils;
import org.apache.sqoop.tool.ImportTool;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;

import static org.apache.sqoop.metastore.hsqldb.HsqldbJobStorage.*;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test the metastore and job-handling features.
 *
 * These all make use of the auto-connect hsqldb-based metastore.
 * The metastore URL is configured to be in-memory, and drop all
 * state between individual tests.
 */
@RunWith(Parameterized.class)
public class SavedJobsTest {

  private static MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();
  private static MSSQLTestUtils msSQLTestUtils = new MSSQLTestUtils();

  @Parameterized.Parameters(name = "metaConnect = {0}, metaUser = {1}, metaPassword = {2}, driverClass = {3}")
  public static Iterable<? extends Object> dbConnectParameters() {
    return Arrays.asList(
            new Object[] {
                    mySQLTestUtils.getHostUrl(), mySQLTestUtils.getUserName(),
                    mySQLTestUtils.getUserPass(), MySQLManager.DRIVER_CLASS
            },
            new Object[] {
                   OracleUtils.CONNECT_STRING, OracleUtils.ORACLE_USER_NAME,
                    OracleUtils.ORACLE_USER_PASS, OracleManager.DRIVER_CLASS
            },
            new Object[] {
                   msSQLTestUtils.getDBConnectString(), msSQLTestUtils.getDBUserName(),
                    msSQLTestUtils.getDBPassWord(), SQLServerManager.DRIVER_CLASS
            },
            new Object[] {
                    System.getProperty(
                            "sqoop.test.postgresql.connectstring.host_url",
                            "jdbc:postgresql://localhost/"),
                    System.getProperty(
                            "sqoop.test.postgresql.connectstring.username",
                            "sqooptest"),
                    System.getProperty(
                            "sqoop.test.postgresql.connectstring.password"),
                    PostgresqlManager.DRIVER_CLASS
            },
            new Object[] {
                    System.getProperty(
                            "sqoop.test.db2.connectstring.host_url",
                            "jdbc:db2://db2host:50000"),
                    System.getProperty(
                            "sqoop.test.db2.connectstring.username",
                            "SQOOP"),
                    System.getProperty(
                            "sqoop.test.db2.connectstring.password",
                            "SQOOP"),
                    Db2Manager.DRIVER_CLASS
            },
            new Object[] { "jdbc:hsqldb:mem:sqoopmetastore", "SA" , "", HsqldbManager.DRIVER_CLASS }
            );
  }

  private String metaConnect;
  private String metaUser;
  private String metaPassword;
  private String driverClass;
  private JobStorage storage;

  private Configuration conf;
  private Map<String, String> descriptor;

  public String INVALID_KEY = "INVALID_KEY";



  public SavedJobsTest(String metaConnect, String metaUser, String metaPassword, String driverClass){
    this.metaConnect = metaConnect;
    this.metaUser = metaUser;
    this.metaPassword = metaPassword;
    this.driverClass = driverClass;
    descriptor = new TreeMap<String, String>();
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
    descriptor.put(META_DRIVER_KEY, driverClass);

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
        if(table.equalsIgnoreCase("SQOOP_ROOT") || table.equalsIgnoreCase("SQOOP_SESSIONS")){
          s.execute("DROP TABLE " + table);
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
    conf.set(META_DRIVER_KEY, driverClass);

    return conf;
  }

  @Test
  public void canAcceptInvalidKeyFalseTest() throws Exception {
    TreeMap<String,String> t = new TreeMap<>();
    t.put(INVALID_KEY, "abc");

    assertEquals("canAccept() should not accept invalid key",
            storage.canAccept(t), false);
  }

  @Test
  public void canAcceptValidKeyTrueTest() throws Exception {
    TreeMap<String,String> t = new TreeMap<>();
    t.put(META_CONNECT_KEY, "abc");

    assertEquals("canAccept should accept valid key", storage.canAccept(t), true);
  }

  @Test(expected = IOException.class)
  public void readJobDoesNotExistThrowsTest() throws IOException{
    String invalidJob = "abcd";

    storage.read(invalidJob);
  }

  @Test
  public void readJobDoesExistPassesTest() throws Exception{
    storage.create("testJob", createTestJobData("abcd"));

    assertEquals("Read did not return job data correctly",
            storage.read("testJob").getSqoopOptions().getTableName(),
            "abcd");
  }

  @Test
  public void updateJobTest() throws  Exception {
    storage.create("testJob2", createTestJobData("abcd"));

    storage.update("testJob2", createTestJobData("efgh") );

    assertEquals("Update did not change data correctly",
            storage.read("testJob2").getSqoopOptions().getTableName(),
            "efgh");
  }

  @Test
  public void listTest() throws IOException {
    storage.create("testJob3", createTestJobData("abcd"));
    storage.create("testJob4", createTestJobData("efgh"));
    storage.create("testJob5", createTestJobData("ijkl"));

    assertThat( storage.list(), hasItems( "testJob3", "testJob4", "testJob5" ));
  }

  @Test
  public void createSameJobTest() throws IOException {

    // Job list should start out empty.
    List<String> jobs = storage.list();
    assertEquals(0, jobs.size());

    // Create a job that displays the version.
    JobData data = new JobData(new SqoopOptions(), new VersionTool());
    storage.create("versionJob", data);

    jobs = storage.list();
    assertEquals(1, jobs.size());
    assertEquals("versionJob", jobs.get(0));

    try {
      // Try to create that same job name again. This should fail.
      thrown.expect(IOException.class);
      thrown.reportMissingExceptionWithMessage("Expected IOException since job already exists");
      storage.create("versionJob", data);
    } finally {
      jobs = storage.list();
      assertEquals(1, jobs.size());

      // Restore our job, check that it exists.
      JobData outData = storage.read("versionJob");
      assertEquals(new VersionTool().getToolName(),
          outData.getSqoopTool().getToolName());
    }
  }

  @Test
  public void deleteJobTest() throws IOException {
    // Job list should start out empty.
    List<String> jobs = storage.list();
    assertEquals(0, jobs.size());

    // Create a job that displays the version.
    JobData data = new JobData(new SqoopOptions(), new VersionTool());
    storage.create("versionJob", data);

    jobs = storage.list();
    assertEquals(1, jobs.size());
    assertEquals("versionJob", jobs.get(0));

    // Now delete the job.
    storage.delete("versionJob");

    // After delete, we should have no jobs.
    jobs = storage.list();
    assertEquals(0, jobs.size());
  }

  @Test
  public void restoreNonExistingJobTest() throws IOException {
      // Try to restore a job that doesn't exist. Watch it fail.
      thrown.expect(IOException.class);
      thrown.reportMissingExceptionWithMessage("Expected IOException since job doesn't exist");
      storage.read("DoesNotExist");
  }

  @Test
    public void createJobWithExtraArgsTest() throws IOException {

        // Job list should start out empty.
        List<String> jobs = storage.list();
        assertEquals(0, jobs.size());

        // Create a job with extra args
        com.cloudera.sqoop.SqoopOptions opts = new SqoopOptions();
        String[] args = {"-schema", "test"};
        opts.setExtraArgs(args);
        JobData data = new JobData(opts, new VersionTool());
        storage.create("versionJob", data);

        jobs = storage.list();
        assertEquals(1, jobs.size());
        assertEquals("versionJob", jobs.get(0));

        // Restore our job, check that it exists.
        JobData outData = storage.read("versionJob");
        assertEquals(new VersionTool().getToolName(),
                outData.getSqoopTool().getToolName());

        String[] storedArgs = outData.getSqoopOptions().getExtraArgs();
        for(int index = 0; index < args.length; ++index) {
            assertEquals(args[index], storedArgs[index]);
        }

        // Now delete the job.
        storage.delete("versionJob");
    }

  @Test
  public void multiConnectionsTest() throws IOException {

    // Job list should start out empty.
    List<String> jobs = storage.list();
    assertEquals(0, jobs.size());

    // Create a job that displays the version.
    JobData data = new JobData(new SqoopOptions(), new VersionTool());
    storage.create("versionJob", data);

    jobs = storage.list();
    assertEquals(1, jobs.size());
    assertEquals("versionJob", jobs.get(0));

    storage.close(); // Close the existing connection

    // Now re-open the storage.
    storage.open(descriptor);

    jobs = storage.list();
    assertEquals(1, jobs.size());
    assertEquals("versionJob", jobs.get(0));

    // Restore our job, check that it exists.
    JobData outData = storage.read("versionJob");
    assertEquals(new VersionTool().getToolName(),
        outData.getSqoopTool().getToolName());
  }

  private com.cloudera.sqoop.metastore.JobData createTestJobData(String setTableName) throws IOException {
    SqoopOptions testOpts = new SqoopOptions();
    testOpts.setTableName(setTableName);
    ImportTool testTool = new ImportTool();
    return new com.cloudera.sqoop.metastore.JobData(testOpts,testTool);

  }
}

