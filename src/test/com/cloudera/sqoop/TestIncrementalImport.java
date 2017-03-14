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

package com.cloudera.sqoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.hive.HiveImport;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.HsqldbManager;
import com.cloudera.sqoop.manager.ManagerFactory;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.metastore.TestSavedJobs;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.JobTool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import static org.junit.Assert.*;

/**
 * Test the incremental import functionality.
 *
 * These all make use of the auto-connect hsqldb-based metastore.
 * The metastore URL is configured to be in-memory, and drop all
 * state between individual tests.
 */

public class TestIncrementalImport  {

  public static final Log LOG = LogFactory.getLog(
      TestIncrementalImport.class.getName());

  // What database do we read from.
  public static final String SOURCE_DB_URL = "jdbc:hsqldb:mem:incremental";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    // Delete db state between tests.
    TestSavedJobs.resetJobSchema();
    resetSourceDataSchema();
  }

  public static void resetSourceDataSchema() throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    TestSavedJobs.resetSchema(options);
  }

  public static Configuration newConf() {
    return TestSavedJobs.newConf();
  }

  /**
   * Assert that a table has a specified number of rows.
   */
  private void assertRowCount(String table, int numRows) throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    ResultSet rs = null;
    try {
      s = c.prepareStatement("SELECT COUNT(*) FROM " + manager.escapeTableName(table));
      rs = s.executeQuery();
      if (!rs.next()) {
        fail("No resultset");
      }
      int realNumRows = rs.getInt(1);
      assertEquals(numRows, realNumRows);
      LOG.info("Expected " + numRows + " rows -- ok.");
    } finally {
      if (null != s) {
        try {
          s.close();
        } catch (SQLException sqlE) {
          LOG.warn("exception: " + sqlE);
        }
      }

      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqlE) {
          LOG.warn("exception: " + sqlE);
        }
      }
    }
  }

  /**
   * Insert rows with id = [low, hi) into tableName.
   */
  private void insertIdRows(String tableName, int low, int hi)
      throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("INSERT INTO " + manager.escapeTableName(tableName) + " VALUES(?)");
      for (int i = low; i < hi; i++) {
        s.setInt(1, i);
        s.executeUpdate();
      }

      c.commit();
    } finally {
      if(s != null) {
        s.close();
      }
    }
  }

  /**
   * Insert rows with id = [low, hi) into tableName with
   * the timestamp column set to the specified ts.
   */
  private void insertIdTimestampRows(String tableName, int low, int hi,
      Timestamp ts) throws SQLException {
    LOG.info("Inserting id rows in [" + low + ", " + hi + ") @ " + ts);
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("INSERT INTO " + manager.escapeTableName(tableName) + " VALUES(?,?)");
      for (int i = low; i < hi; i++) {
        s.setInt(1, i);
        s.setTimestamp(2, ts);
        s.executeUpdate();
      }

      c.commit();
    } finally {
      s.close();
    }
  }

  /**
   * Insert rows with id = [low, hi) into tableName with
   * id converted to string.
   */
  private void insertIdVarcharRows(String tableName, int low, int hi)
      throws SQLException {
    LOG.info("Inserting rows in [" + low + ", " + hi + ")");
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("INSERT INTO " + manager.escapeTableName(tableName) + " VALUES(?)");
      for (int i = low; i < hi; i++) {
        s.setString(1, Integer.toString(i));
        s.executeUpdate();
      }
      c.commit();
    } finally {
      s.close();
    }
  }

  /**
   * Create a table with an 'id' column full of integers.
   */
  private void createIdTable(String tableName, int insertRows)
      throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("CREATE TABLE " + manager.escapeTableName(tableName) + "(id INT NOT NULL)");
      s.executeUpdate();
      c.commit();
      insertIdRows(tableName, 0, insertRows);
    } finally {
      s.close();
    }
  }

  /**
   * Create a table with an 'id' column full of integers and a
   * last_modified column with timestamps.
   */
  private void createTimestampTable(String tableName, int insertRows,
      Timestamp baseTime) throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("CREATE TABLE " + manager.escapeTableName(tableName) + "(id INT NOT NULL, "
          + "last_modified TIMESTAMP)");
      s.executeUpdate();
      c.commit();
      insertIdTimestampRows(tableName, 0, insertRows, baseTime);
    } finally {
      s.close();
    }
  }

  /**
   * Create a table with an 'id' column of type varchar(20)
   */
  private void createIdVarcharTable(String tableName,
       int insertRows) throws SQLException {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("CREATE TABLE " + manager.escapeTableName(tableName) + "(id varchar(20) NOT NULL)");
      s.executeUpdate();
      c.commit();
      insertIdVarcharRows(tableName, 0, insertRows);
    } finally {
      s.close();
    }
  }

  /**
   * Delete all files in a directory for a table.
   */
  public void clearDir(String tableName) {
    try {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
      Path tableDir = new Path(warehouse, tableName);
      fs.delete(tableDir, true);
    } catch (Exception e) {
      fail("Got unexpected exception: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Look at a directory that should contain files full of an imported 'id'
   * column. Assert that all numbers in [0, expectedNums) are present
   * in order.
   */
  public void assertDirOfNumbers(String tableName, int expectedNums) {
    try {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
      Path tableDir = new Path(warehouse, tableName);
      FileStatus [] stats = fs.listStatus(tableDir);
      String [] fileNames = new String[stats.length];
      for (int i = 0; i < stats.length; i++) {
        fileNames[i] = stats[i].getPath().toString();
      }

      Arrays.sort(fileNames);

      // Read all the files in sorted order, adding the value lines to the list.
      List<String> receivedNums = new ArrayList<String>();
      for (String fileName : fileNames) {
        if (fileName.startsWith("_") || fileName.startsWith(".")) {
          continue;
        }

        BufferedReader r = new BufferedReader(
            new InputStreamReader(fs.open(new Path(fileName))));
        try {
          while (true) {
            String s = r.readLine();
            if (null == s) {
              break;
            }

            receivedNums.add(s.trim());
          }
        } finally {
          r.close();
        }
      }

      assertEquals(expectedNums, receivedNums.size());

      // Compare the received values with the expected set.
      for (int i = 0; i < expectedNums; i++) {
        assertEquals((int) i, (int) Integer.valueOf(receivedNums.get(i)));
      }
    } catch (Exception e) {
      fail("Got unexpected exception: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Look at a directory that should contain files full of an imported 'id'
   * column and 'last_modified' column. Assert that all numbers in [0, expectedNums) are present
   * in order.
   */
  public void assertDirOfNumbersAndTimestamps(String tableName, int expectedNums) {
    try {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
      Path tableDir = new Path(warehouse, tableName);
      FileStatus [] stats = fs.listStatus(tableDir);
      String [] fileNames = new String[stats.length];
      for (int i = 0; i < stats.length; i++) {
        fileNames[i] = stats[i].getPath().toString();
      }

      Arrays.sort(fileNames);

      // Read all the files in sorted order, adding the value lines to the list.
      List<String> receivedNums = new ArrayList<String>();
      for (String fileName : fileNames) {
        if (fileName.startsWith("_") || fileName.startsWith(".")) {
          continue;
        }

        BufferedReader r = new BufferedReader(
            new InputStreamReader(fs.open(new Path(fileName))));
        try {
          while (true) {
            String s = r.readLine();
            if (null == s) {
              break;
            }

            receivedNums.add(s.trim());
          }
        } finally {
          r.close();
        }
      }

      assertEquals(expectedNums, receivedNums.size());

      // Compare the received values with the expected set.
      for (int i = 0; i < expectedNums; i++) {
        assertEquals((int) i, (int) Integer.valueOf(receivedNums.get(i).split(",")[0]));
      }
    } catch (Exception e) {
      fail("Got unexpected exception: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Assert that a directory contains a file with exactly one line
   * in it, containing the prescribed number 'val'.
   */
  public void assertFirstSpecificNumber(String tableName, int val) {
    try {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
      Path tableDir = new Path(warehouse, tableName);
      FileStatus [] stats = fs.listStatus(tableDir);
      String [] filePaths = new String[stats.length];
      for (int i = 0; i < stats.length; i++) {
        filePaths[i] = stats[i].getPath().toString();
      }

      // Read the first file that is not a hidden file.
      boolean foundVal = false;
      for (String filePath : filePaths) {
        String fileName = new Path(filePath).getName();
        if (fileName.startsWith("_") || fileName.startsWith(".")) {
          continue;
        }

        if (foundVal) {
          // Make sure we don't have two or more "real" files in the dir.
          fail("Got an extra data-containing file in this directory.");
        }

        BufferedReader r = new BufferedReader(
            new InputStreamReader(fs.open(new Path(filePath))));
        try {
          String s = r.readLine();
          if (null == s) {
            fail("Unexpected empty file " + filePath + ".");
          }
          assertEquals(val, (int) Integer.valueOf(s.trim()));

          String nextLine = r.readLine();
          if (nextLine != null) {
            fail("Expected only one result, but got another line: " + nextLine);
          }

          // Successfully got the value we were looking for.
          foundVal = true;
        } finally {
          r.close();
        }
      }
    } catch (IOException e) {
      fail("Got unexpected exception: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Assert that a directory contains a file with exactly one line
   * in it, containing the prescribed number 'val'.
   */
  public void assertSpecificNumber(String tableName, int val) {
    try {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
      Path tableDir = new Path(warehouse, tableName);
      FileStatus [] stats = fs.listStatus(tableDir);
      String [] filePaths = new String[stats.length];
      for (int i = 0; i < stats.length; i++) {
        filePaths[i] = stats[i].getPath().toString();
      }

      // Read the first file that is not a hidden file.
      boolean foundVal = false;
      for (String filePath : filePaths) {
        String fileName = new Path(filePath).getName();
        if (fileName.startsWith("_") || fileName.startsWith(".")) {
          continue;
        }

        if (foundVal) {
          // Make sure we don't have two or more "real" files in the dir.
          fail("Got an extra data-containing file in this directory.");
        }

        BufferedReader r = new BufferedReader(
            new InputStreamReader(fs.open(new Path(filePath))));
        try {
          String s = r.readLine();
          if (val == (int) Integer.valueOf(s.trim().split(",")[0])) {
            if (foundVal) {
              fail("Expected only one result, but got another line: " + s);
            }
            foundVal = true;
          }
        } finally {
          r.close();
        }
      }
    } catch (IOException e) {
      fail("Got unexpected exception: " + StringUtils.stringifyException(e));
    }
  }

  public void runImport(SqoopOptions options, List<String> args) {
    try {
      Sqoop importer = new Sqoop(new ImportTool(), options.getConf(), options);
      int ret = Sqoop.runSqoop(importer, args.toArray(new String[0]));
      assertEquals("Failure during job", 0, ret);
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: "
          + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  /**
   * Return a list of arguments to import the specified table.
   */
  private List<String> getArgListForTable(String tableName, boolean commonArgs,
      boolean isAppend) {
    return getArgListForTable(tableName, commonArgs, isAppend, false);
  }

  /**
   * Return a list of arguments to import the specified table.
   */
  private List<String> getArgListForTable(String tableName, boolean commonArgs,
      boolean isAppend, boolean appendTimestamp) {
    List<String> args = new ArrayList<String>();
    if (commonArgs) {
      CommonArgs.addHadoopFlags(args);
    }
    args.add("--connect");
    args.add(SOURCE_DB_URL);
    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
    if (isAppend) {
      args.add("--incremental");
      args.add("append");
      if (!appendTimestamp) {
        args.add("--check-column");
        args.add("ID");
      } else {
        args.add("--check-column");
        args.add("LAST_MODIFIED");
      }
    } else {
      args.add("--incremental");
      args.add("lastmodified");
      args.add("--check-column");
      args.add("LAST_MODIFIED");
    }
    args.add("--columns");
    args.add("ID");
    args.add("-m");
    args.add("1");

    return args;
  }

  /**
   * Return list of arguments to import by query.
   * @return
   */
  private List<String> getArgListForQuery(String query, String directoryName,
    boolean commonArgs, boolean isAppend, boolean appendTimestamp) {
    List<String> args = new ArrayList<String>();
    if (commonArgs) {
      CommonArgs.addHadoopFlags(args);
    }

    String [] directoryNames = directoryName.split("/");
    String className = directoryNames[directoryNames.length -1];

    args.add("--connect");
    args.add(SOURCE_DB_URL);
    args.add("--query");
    args.add(query);
    args.add("--class-name");
    args.add(className);
    args.add("--target-dir");
    args.add(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR
      + System.getProperty("file.separator") + directoryName);
    if (isAppend) {
      args.add("--incremental");
      args.add("append");
      if (!appendTimestamp) {
        args.add("--check-column");
        args.add("ID");
      } else {
        args.add("--check-column");
        args.add("LAST_MODIFIED");
      }
    } else {
      args.add("--incremental");
      args.add("lastmodified");
      args.add("--check-column");
      args.add("LAST_MODIFIED");
    }
    args.add("-m");
    args.add("1");

    return args;
  }
  /**
   * Create a job with the specified name, where the job performs
   * an import configured with 'jobArgs'.
   */
  private void createJob(String jobName, List<String> jobArgs) {
    createJob(jobName, jobArgs, newConf());
  }

  /**
   * Create a job with the specified name, where the job performs
   * an import configured with 'jobArgs', using the provided configuration
   * as defaults.
   */
  private void createJob(String jobName, List<String> jobArgs,
      Configuration conf) {
    try {
      SqoopOptions options = new SqoopOptions();
      options.setConf(conf);
      Sqoop makeJob = new Sqoop(new JobTool(), conf, options);

      List<String> args = new ArrayList<String>();
      args.add("--create");
      args.add(jobName);
      args.add("--");
      args.add("import");
      args.addAll(jobArgs);

      int ret = Sqoop.runSqoop(makeJob, args.toArray(new String[0]));
      assertEquals("Failure to create job", 0, ret);
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop to create job: "
          + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  /**
   * Run the specified job.
   */
  private void runJob(String jobName) {
    runJob(jobName, newConf());
  }

  /**
   * Run the specified job.
   */
  private void runJob(String jobName, Configuration conf) {
    try {
      SqoopOptions options = new SqoopOptions();
      options.setConf(conf);
      Sqoop runJob = new Sqoop(new JobTool(), conf, options);

      List<String> args = new ArrayList<String>();
      args.add("--exec");
      args.add(jobName);

      int ret = Sqoop.runSqoop(runJob, args.toArray(new String[0]));
      assertEquals("Failure to run job", 0, ret);
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop to run job: "
          + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  // Incremental import of an empty table, no metastore.
  @Test
  public void testEmptyAppendImport() throws Exception {
    final String TABLE_NAME = "emptyAppend1";
    createIdTable(TABLE_NAME, 0);
    List<String> args = getArgListForTable(TABLE_NAME, true, true);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);

    assertDirOfNumbers(TABLE_NAME, 0);
  }

  // Incremental import of a filled table, no metastore.
  @Test
  public void testFullAppendImport() throws Exception {
    final String TABLE_NAME = "fullAppend1";
    createIdTable(TABLE_NAME, 10);
    List<String> args = getArgListForTable(TABLE_NAME, true, true);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);

    assertDirOfNumbers(TABLE_NAME, 10);
  }

  @Test
  public void testEmptyJobAppend() throws Exception {
    // Create a job and run an import on an empty table.
    // Nothing should happen.

    final String TABLE_NAME = "emptyJob";
    createIdTable(TABLE_NAME, 0);

    List<String> args = getArgListForTable(TABLE_NAME, false, true);
    createJob("emptyJob", args);
    runJob("emptyJob");
    assertDirOfNumbers(TABLE_NAME, 0);

    // Running the job a second time should result in
    // nothing happening, it's still empty.
    runJob("emptyJob");
    assertDirOfNumbers(TABLE_NAME, 0);
  }

  @Test
  public void testEmptyThenFullJobAppend() throws Exception {
    // Create an empty table. Import it; nothing happens.
    // Add some rows. Verify they are appended.

    final String TABLE_NAME = "emptyThenFull";
    createIdTable(TABLE_NAME, 0);

    List<String> args = getArgListForTable(TABLE_NAME, false, true);
    createJob(TABLE_NAME, args);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 0);

    // Now add some rows.
    insertIdRows(TABLE_NAME, 0, 10);

    // Running the job a second time should import 10 rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Add some more rows.
    insertIdRows(TABLE_NAME, 10, 20);

    // Import only those rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 20);
  }

  @Test
  public void testEmptyThenFullJobAppendWithQuery() throws Exception {
    // Create an empty table. Import it; nothing happens.
    // Add some rows. Verify they are appended.

    final String TABLE_NAME = "withQuery";
    createIdTable(TABLE_NAME, 0);
    clearDir(TABLE_NAME);

    final String QUERY = "SELECT id FROM \"withQuery\" WHERE $CONDITIONS";

    List<String> args = getArgListForQuery(QUERY, TABLE_NAME,
      false, true, false);
    createJob(TABLE_NAME, args);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 0);

    // Now add some rows.
    insertIdRows(TABLE_NAME, 0, 10);

    // Running the job a second time should import 10 rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Add some more rows.
    insertIdRows(TABLE_NAME, 10, 20);

    // Import only those rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 20);
  }

  @Test
  public void testAppend() throws Exception {
    // Create a table with data in it; import it.
    // Then add more data, verify that only the incremental data is pulled.

    final String TABLE_NAME = "append";
    createIdTable(TABLE_NAME, 10);

    List<String> args = getArgListForTable(TABLE_NAME, false, true);
    createJob(TABLE_NAME, args);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Add some more rows.
    insertIdRows(TABLE_NAME, 10, 20);

    // Import only those rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 20);
  }

  @Test
  public void testEmptyLastModified() throws Exception {
    final String TABLE_NAME = "emptyLastModified";
    createTimestampTable(TABLE_NAME, 0, null);
    List<String> args = getArgListForTable(TABLE_NAME, true, false);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);

    assertDirOfNumbers(TABLE_NAME, 0);
  }

  @Test
  public void testEmptyLastModifiedWithNonExistingParentDirectory() throws Exception {
    final String TABLE_NAME = "emptyLastModifiedNoParent";
    final String QUERY = "SELECT id, last_modified FROM \"" + TABLE_NAME + "\" WHERE $CONDITIONS";
    final String DIRECTORY = "non-existing/parents/" + TABLE_NAME;
    createTimestampTable(TABLE_NAME, 0, null);
    List<String> args = getArgListForQuery(QUERY, DIRECTORY, true, false, false);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);

    assertDirOfNumbers(DIRECTORY, 0);
  }

  @Test
  public void testFullLastModifiedImport() throws Exception {
    // Given a table of rows imported in the past,
    // see that they are imported.
    final String TABLE_NAME = "fullLastModified";
    Timestamp thePast = new Timestamp(System.currentTimeMillis() - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    List<String> args = getArgListForTable(TABLE_NAME, true, false);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);

    assertDirOfNumbers(TABLE_NAME, 10);
  }

  @Test
  public void testNoImportFromTheFuture() throws Exception {
    // If last-modified dates for writes are serialized to be in the
    // future w.r.t. an import, do not import these rows.

    final String TABLE_NAME = "futureLastModified";
    Timestamp theFuture = new Timestamp(System.currentTimeMillis() + 1000000);
    createTimestampTable(TABLE_NAME, 10, theFuture);

    List<String> args = getArgListForTable(TABLE_NAME, true, false);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);

    assertDirOfNumbers(TABLE_NAME, 0);
  }

  @Test
  public void testEmptyJobLastMod() throws Exception {
    // Create a job and run an import on an empty table.
    // Nothing should happen.

    final String TABLE_NAME = "emptyJobLastMod";
    createTimestampTable(TABLE_NAME, 0, null);

    List<String> args = getArgListForTable(TABLE_NAME, false, false);
    args.add("--append");
    createJob("emptyJobLastMod", args);
    runJob("emptyJobLastMod");
    assertDirOfNumbers(TABLE_NAME, 0);

    // Running the job a second time should result in
    // nothing happening, it's still empty.
    runJob("emptyJobLastMod");
    assertDirOfNumbers(TABLE_NAME, 0);
  }

  @Test
  public void testEmptyThenFullJobLastMod() throws Exception {
    // Create an empty table. Import it; nothing happens.
    // Add some rows. Verify they are appended.

    final String TABLE_NAME = "emptyThenFullTimestamp";
    createTimestampTable(TABLE_NAME, 0, null);

    List<String> args = getArgListForTable(TABLE_NAME, false, false);
    args.add("--append");
    createJob(TABLE_NAME, args);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 0);

    long importWasBefore = System.currentTimeMillis();

    // Let some time elapse.
    Thread.sleep(50);

    long rowsAddedTime = System.currentTimeMillis() - 5;

    // Check: we are adding rows after the previous import time
    // and before the current time.
    assertTrue(rowsAddedTime > importWasBefore);
    assertTrue(rowsAddedTime < System.currentTimeMillis());

    insertIdTimestampRows(TABLE_NAME, 0, 10, new Timestamp(rowsAddedTime));

    // Running the job a second time should import 10 rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Add some more rows.
    importWasBefore = System.currentTimeMillis();
    Thread.sleep(50);
    rowsAddedTime = System.currentTimeMillis() - 5;
    assertTrue(rowsAddedTime > importWasBefore);
    assertTrue(rowsAddedTime < System.currentTimeMillis());
    insertIdTimestampRows(TABLE_NAME, 10, 20, new Timestamp(rowsAddedTime));

    // Import only those rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 20);
  }

  @Test
  public void testAppendWithTimestamp() throws Exception {
    // Create a table with data in it; import it.
    // Then add more data, verify that only the incremental data is pulled.

    final String TABLE_NAME = "appendTimestamp";
    Timestamp thePast = new Timestamp(System.currentTimeMillis() - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    List<String> args = getArgListForTable(TABLE_NAME, false, false);
    args.add("--append");
    createJob(TABLE_NAME, args);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Add some more rows.
    long importWasBefore = System.currentTimeMillis();
    Thread.sleep(50);
    long rowsAddedTime = System.currentTimeMillis() - 5;
    assertTrue(rowsAddedTime > importWasBefore);
    assertTrue(rowsAddedTime < System.currentTimeMillis());
    insertIdTimestampRows(TABLE_NAME, 10, 20, new Timestamp(rowsAddedTime));

    // Import only those rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 20);
  }

  @Test
  public void testAppendWithString() throws Exception {
    // Create a table with string column in it;
    // incrementally import it on the string column - it should fail.

    final String TABLE_NAME = "appendString";
    createIdVarcharTable(TABLE_NAME, 10);

    List<String> args = getArgListForTable(TABLE_NAME, false, true);
    args.add("--append");
    createJob(TABLE_NAME, args);

    thrown.expect(RuntimeException.class);
    thrown.reportMissingExceptionWithMessage("Expected incremental import on varchar column to fail");
    runJob(TABLE_NAME);
  }

  @Test
  public void testModifyWithTimestamp() throws Exception {
    // Create a table with data in it; import it.
    // Then modify some existing rows, and verify that we only grab
    // those rows.

    final String TABLE_NAME = "modifyTimestamp";
    Timestamp thePast = new Timestamp(System.currentTimeMillis() - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    List<String> args = getArgListForTable(TABLE_NAME, false, false);
    createJob(TABLE_NAME, args);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Modify a row.
    long importWasBefore = System.currentTimeMillis();
    Thread.sleep(50);
    long rowsAddedTime = System.currentTimeMillis() - 5;
    assertTrue(rowsAddedTime > importWasBefore);
    assertTrue(rowsAddedTime < System.currentTimeMillis());
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("UPDATE " + manager.escapeTableName(TABLE_NAME) + " SET id=?, last_modified=? WHERE id=?");
      s.setInt(1, 4000); // the first row should have '4000' in it now.
      s.setTimestamp(2, new Timestamp(rowsAddedTime));
      s.setInt(3, 0);
      s.executeUpdate();
      c.commit();
    } finally {
      s.close();
    }

    // Import only the new row.
    clearDir(TABLE_NAME);
    runJob(TABLE_NAME);
    assertFirstSpecificNumber(TABLE_NAME, 4000);
  }
  @Test
  public void testUpdateModifyWithTimestamp() throws Exception {
    // Create a table with data in it; import it.
    // Then modify some existing rows, and verify that we only grab
    // those rows.

    final String TABLE_NAME = "updateModifyTimestamp";
    Timestamp thePast = new Timestamp(System.currentTimeMillis() - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    List<String> args = getArgListForTable(TABLE_NAME, false, false);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Modify a row.
    long importWasBefore = System.currentTimeMillis();
    Thread.sleep(50);
    long rowsAddedTime = System.currentTimeMillis() - 5;
    assertTrue(rowsAddedTime > importWasBefore);
    assertTrue(rowsAddedTime < System.currentTimeMillis());
    SqoopOptions options2 = new SqoopOptions();
    options2.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options2);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("UPDATE " + manager.escapeTableName(TABLE_NAME) + " SET id=?, last_modified=? WHERE id=?");
      s.setInt(1, 4000); // the first row should have '4000' in it now.
      s.setTimestamp(2, new Timestamp(rowsAddedTime));
      s.setInt(3, 0);
      s.executeUpdate();
      c.commit();
    } finally {
      s.close();
    }

    // Update the new row.
    args.add("--last-value");
    args.add(new Timestamp(importWasBefore).toString());
    args.add("--merge-key");
    args.add("ID");
    conf = newConf();
    options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);
    assertSpecificNumber(TABLE_NAME, 4000);
  }

  @Test
  public void testUpdateModifyWithTimestampWithQuery() throws Exception {
    // Create an empty table. Import it; nothing happens.
    // Add some rows. Verify they are appended.

    final String TABLE_NAME = "UpdateModifyWithTimestampWithQuery";
    Timestamp thePast = new Timestamp(System.currentTimeMillis() - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    final String QUERY = "SELECT id, last_modified FROM \"UpdateModifyWithTimestampWithQuery\" WHERE $CONDITIONS";

    List<String> args = getArgListForQuery(QUERY, TABLE_NAME,
        true, false, false);

    Configuration conf = newConf();
    SqoopOptions options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);
    assertDirOfNumbersAndTimestamps(TABLE_NAME, 10);

    // Modify a row.
    long importWasBefore = System.currentTimeMillis();
    Thread.sleep(50);
    long rowsAddedTime = System.currentTimeMillis() - 5;
    assertTrue(rowsAddedTime > importWasBefore);
    assertTrue(rowsAddedTime < System.currentTimeMillis());
    SqoopOptions options2 = new SqoopOptions();
    options2.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options2);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("UPDATE " + manager.escapeTableName(TABLE_NAME) + " SET id=?, last_modified=? WHERE id=?");
      s.setInt(1, 4000); // the first row should have '4000' in it now.
      s.setTimestamp(2, new Timestamp(rowsAddedTime));
      s.setInt(3, 0);
      s.executeUpdate();
      c.commit();
    } finally {
      s.close();
    }

    // Update the new row.
    args.add("--last-value");
    args.add(new Timestamp(importWasBefore).toString());
    args.add("--merge-key");
    args.add("ID");
    conf = newConf();
    options = new SqoopOptions();
    options.setConf(conf);
    runImport(options, args);
    assertSpecificNumber(TABLE_NAME, 4000);
  }

  @Test
  public void testUpdateModifyWithTimestampJob() throws Exception {
    // Create a table with data in it; import it.
    // Then modify some existing rows, and verify that we only grab
    // those rows.

    final String TABLE_NAME = "updateModifyTimestampJob";
    Timestamp thePast = new Timestamp(System.currentTimeMillis() - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    List<String> args = getArgListForTable(TABLE_NAME, false, false);
    args.add("--merge-key");
    args.add("ID");
    createJob(TABLE_NAME, args);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Modify a row.
    long importWasBefore = System.currentTimeMillis();
    Thread.sleep(50);
    long rowsAddedTime = System.currentTimeMillis() - 5;
    assertTrue(rowsAddedTime > importWasBefore);
    assertTrue(rowsAddedTime < System.currentTimeMillis());
    SqoopOptions options2 = new SqoopOptions();
    options2.setConnectString(SOURCE_DB_URL);
    HsqldbManager manager = new HsqldbManager(options2);
    Connection c = manager.getConnection();
    PreparedStatement s = null;
    try {
      s = c.prepareStatement("UPDATE " + manager.escapeTableName(TABLE_NAME) + " SET id=?, last_modified=? WHERE id=?");
      s.setInt(1, 4000); // the first row should have '4000' in it now.
      s.setTimestamp(2, new Timestamp(rowsAddedTime));
      s.setInt(3, 0);
      s.executeUpdate();
      c.commit();
    } finally {
      s.close();
    }

    // Update the new row.
    runJob(TABLE_NAME);
    assertSpecificNumber(TABLE_NAME, 4000);
  }

  /**
   * ManagerFactory returning an HSQLDB ConnManager which allows you to
   * specify the current database timestamp.
   */
  public static class InstrumentHsqldbManagerFactory extends ManagerFactory {
    @Override
    public ConnManager accept(JobData data) {
      LOG.info("Using instrumented manager");
      return new InstrumentHsqldbManager(data.getSqoopOptions());
    }
  }

  /**
   * Hsqldb ConnManager that lets you set the current reported timestamp
   * from the database, to allow testing of boundary conditions for imports.
   */
  public static class InstrumentHsqldbManager extends HsqldbManager {
    private static Timestamp curTimestamp;

    public InstrumentHsqldbManager(SqoopOptions options) {
      super(options);
    }

    @Override
    public Timestamp getCurrentDbTimestamp() {
      return InstrumentHsqldbManager.curTimestamp;
    }

    public static void setCurrentDbTimestamp(Timestamp t) {
      InstrumentHsqldbManager.curTimestamp = t;
    }
  }

  @Test
  public void testTimestampBoundary() throws Exception {
    // Run an import, and then insert rows with the last-modified timestamp
    // set to the exact time when the first import runs. Run a second import
    // and ensure that we pick up the new data.

    long now = System.currentTimeMillis();

    final String TABLE_NAME = "boundaryTimestamp";
    Timestamp thePast = new Timestamp(now - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    Timestamp firstJobTime = new Timestamp(now);
    InstrumentHsqldbManager.setCurrentDbTimestamp(firstJobTime);

    // Configure the job to use the instrumented Hsqldb manager.
    Configuration conf = newConf();
    conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY,
        InstrumentHsqldbManagerFactory.class.getName());

    List<String> args = getArgListForTable(TABLE_NAME, false, false);
    args.add("--append");
    createJob(TABLE_NAME, args, conf);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Add some more rows with the timestamp equal to the job run timestamp.
    insertIdTimestampRows(TABLE_NAME, 10, 20, firstJobTime);
    assertRowCount(TABLE_NAME, 20);

    // Run a second job with the clock advanced by 100 ms.
    Timestamp secondJobTime = new Timestamp(now + 100);
    InstrumentHsqldbManager.setCurrentDbTimestamp(secondJobTime);

    // Import only those rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 20);
  }

  @Test
  public void testIncrementalAppendTimestamp() throws Exception {
    // Run an import, and then insert rows with the last-modified timestamp
    // set to the exact time when the first import runs. Run a second import
    // and ensure that we pick up the new data.

    long now = System.currentTimeMillis();

    final String TABLE_NAME = "incrementalAppendTimestamp";
    Timestamp thePast = new Timestamp(now - 100);
    createTimestampTable(TABLE_NAME, 10, thePast);

    Timestamp firstJobTime = new Timestamp(now);
    InstrumentHsqldbManager.setCurrentDbTimestamp(firstJobTime);

    // Configure the job to use the instrumented Hsqldb manager.
    Configuration conf = newConf();
    conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY,
        InstrumentHsqldbManagerFactory.class.getName());

    List<String> args = getArgListForTable(TABLE_NAME, false, true, true);
    createJob(TABLE_NAME, args, conf);
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 10);

    // Add some more rows with the timestamp equal to the job run timestamp.
    insertIdTimestampRows(TABLE_NAME, 10, 20, firstJobTime);
    assertRowCount(TABLE_NAME, 20);

    // Run a second job with the clock advanced by 100 ms.
    Timestamp secondJobTime = new Timestamp(now + 100);
    InstrumentHsqldbManager.setCurrentDbTimestamp(secondJobTime);

    // Import only those rows.
    runJob(TABLE_NAME);
    assertDirOfNumbers(TABLE_NAME, 20);
  }
  @Test
	public void testIncrementalHiveAppendEmptyThenFull() throws Exception {
		// This is to test Incremental Hive append feature. SQOOP-2470
		final String TABLE_NAME = "incrementalHiveAppendEmptyThenFull";
		Configuration conf = newConf();
		conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY,
				InstrumentHsqldbManagerFactory.class.getName());
		clearDir(TABLE_NAME);
		createIdTable(TABLE_NAME, 0);
		List<String> args = new ArrayList<String>();
		args.add("--connect");
		args.add(SOURCE_DB_URL);
		args.add("--table");
		args.add(TABLE_NAME);
		args.add("--warehouse-dir");
		args.add(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
		args.add("--hive-import");
		args.add("--hive-table");
		args.add(TABLE_NAME + "hive");
		args.add("--incremental");
		args.add("append");
		args.add("--check-column");
		args.add("ID");
		args.add("-m");
		args.add("1");
		createJob(TABLE_NAME, args, conf);
		HiveImport.setTestMode(true);
		String hiveHome = org.apache.sqoop.SqoopOptions.getHiveHomeDefault();
		assertNotNull("hive.home was not set", hiveHome);
		String testDataPath = new Path(new Path(hiveHome), "scripts/"
				+ "incrementalHiveAppendEmpty.q").toString();
		System.clearProperty("expected.script");
		System.setProperty("expected.script",
				new File(testDataPath).getAbsolutePath());
		runJob(TABLE_NAME);
		assertDirOfNumbers(TABLE_NAME, 0);
		// Now add some rows.
		insertIdRows(TABLE_NAME, 0, 10);
		String testDataPath10 = new Path(new Path(hiveHome), "scripts/"
				+ "incrementalHiveAppend10.q").toString();
		System.clearProperty("expected.script");
		System.setProperty("expected.script",
				new File(testDataPath10).getAbsolutePath());
		System.getProperty("expected.script");
		// Running the job a second time should import 10 rows.
		runJob(TABLE_NAME);
		assertDirOfNumbers(TABLE_NAME, 10);
		// Add some more rows.
		insertIdRows(TABLE_NAME, 10, 20);
		String testDataPath20 = new Path(new Path(hiveHome), "scripts/"
				+ "incrementalHiveAppend20.q").toString();
		System.clearProperty("expected.script");
		System.setProperty("expected.script",
				new File(testDataPath20).getAbsolutePath());
		// Import only those rows.
		runJob(TABLE_NAME);
		assertDirOfNumbers(TABLE_NAME, 20);
	}

  // SQOOP-1890
  @Test
  public void testTableNameWithSpecialCharacters() throws Exception {
    // Table name with special characters to verify proper table name escaping
    final String TABLE_NAME = "my-table.ext";
    createIdTable(TABLE_NAME, 0);

    // Now add some rows.
    insertIdRows(TABLE_NAME, 0, 10);

    List<String> args = getArgListForTable(TABLE_NAME, false, true);
    createJob("emptyJob", args);
    runJob("emptyJob");
    assertDirOfNumbers(TABLE_NAME, 10);
  }

}

