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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.HsqldbManager;
import com.cloudera.sqoop.metastore.hsqldb.AutoHsqldbStorage;
import com.cloudera.sqoop.tool.VersionTool;

import junit.framework.TestCase;

import java.io.IOException;
import java.sql.Connection;

/**
 * Test the metastore and job-handling features.
 *
 * These all make use of the auto-connect hsqldb-based metastore.
 * The metastore URL is configured to be in-memory, and drop all
 * state between individual tests.
 */
public class TestSavedJobs extends TestCase {

  public static final String TEST_AUTOCONNECT_URL =
      "jdbc:hsqldb:mem:sqoopmetastore";
  public static final String TEST_AUTOCONNECT_USER = "SA";
  public static final String TEST_AUTOCONNECT_PASS = "";

  @Override
  public void setUp() throws Exception {
    // Delete db state between tests.
    resetJobSchema();
  }

  public static void resetJobSchema() throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(TEST_AUTOCONNECT_URL);
    options.setUsername(TEST_AUTOCONNECT_USER);
    options.setPassword(TEST_AUTOCONNECT_PASS);

    resetSchema(options);
  }

  /**
   * Drop all tables in the configured HSQLDB-based schema/user/pass.
   */
  public static void resetSchema(SqoopOptions options) throws SQLException {
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    Statement s = c.createStatement();
    try {
      String [] tables = manager.listTables();
      for (String table : tables) {
        s.executeUpdate("DROP TABLE " + manager.escapeTableName(table));
      }

      c.commit();
    } finally {
      s.close();
    }
  }

  public static Configuration newConf() {
    Configuration conf = new Configuration();
    conf.set(AutoHsqldbStorage.AUTO_STORAGE_USER_KEY, TEST_AUTOCONNECT_USER);
    conf.set(AutoHsqldbStorage.AUTO_STORAGE_PASS_KEY, TEST_AUTOCONNECT_PASS);
    conf.set(AutoHsqldbStorage.AUTO_STORAGE_CONNECT_STRING_KEY,
        TEST_AUTOCONNECT_URL);

    return conf;
  }

  public void testAutoConnect() throws IOException {
    // By default, we should be able to auto-connect with an
    // empty connection descriptor. We should see an empty
    // job set.

    Configuration conf = newConf();
    JobStorageFactory ssf = new JobStorageFactory(conf);

    Map<String, String> descriptor = new TreeMap<String, String>();
    JobStorage storage = ssf.getJobStorage(descriptor);

    storage.open(descriptor);
    List<String> jobs = storage.list();
    assertEquals(0, jobs.size());
    storage.close();
  }

  public void testCreateDeleteJob() throws IOException {
    Configuration conf = newConf();
    JobStorageFactory ssf = new JobStorageFactory(conf);

    Map<String, String> descriptor = new TreeMap<String, String>();
    JobStorage storage = ssf.getJobStorage(descriptor);

    storage.open(descriptor);

    // Job list should start out empty.
    List<String> jobs = storage.list();
    assertEquals(0, jobs.size());

    // Create a job that displays the version.
    JobData data = new JobData(new SqoopOptions(), new VersionTool());
    storage.create("versionJob", data);

    jobs = storage.list();
    assertEquals(1, jobs.size());
    assertEquals("versionJob", jobs.get(0));

    // Try to create that same job name again. This should fail.
    try {
      storage.create("versionJob", data);
      fail("Expected IOException; this job already exists.");
    } catch (IOException ioe) {
      // This is expected; continue operation.
    }

    jobs = storage.list();
    assertEquals(1, jobs.size());

    // Restore our job, check that it exists.
    JobData outData = storage.read("versionJob");
    assertEquals(new VersionTool().getToolName(),
        outData.getSqoopTool().getToolName());

    // Try to restore a job that doesn't exist. Watch it fail.
    try {
      storage.read("DoesNotExist");
      fail("Expected IOException");
    } catch (IOException ioe) {
      // This is expected. Continue.
    }

    // Now delete the job.
    storage.delete("versionJob");

    // After delete, we should have no jobs.
    jobs = storage.list();
    assertEquals(0, jobs.size());

    storage.close();
  }

  public void testMultiConnections() throws IOException {
    // Ensure that a job can be retrieved when the storage is
    // closed and reopened.

    Configuration conf = newConf();
    JobStorageFactory ssf = new JobStorageFactory(conf);

    Map<String, String> descriptor = new TreeMap<String, String>();
    JobStorage storage = ssf.getJobStorage(descriptor);

    storage.open(descriptor);

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
    ssf = new JobStorageFactory(newConf());
    storage = ssf.getJobStorage(descriptor);
    storage.open(descriptor);

    jobs = storage.list();
    assertEquals(1, jobs.size());
    assertEquals("versionJob", jobs.get(0));

    // Restore our job, check that it exists.
    JobData outData = storage.read("versionJob");
    assertEquals(new VersionTool().getToolName(),
        outData.getSqoopTool().getToolName());

    storage.close();
  }
}

