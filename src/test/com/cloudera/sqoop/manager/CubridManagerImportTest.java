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

package com.cloudera.sqoop.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.sqoop.manager.CubridManager;
import org.apache.sqoop.manager.cubrid.CubridTestUtils;
import org.apache.sqoop.util.FileListing;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Test the CubridManager implementation.
 *
 * This uses JDBC to import data from an Cubrid database into HDFS.
 *
 * Since this requires an Cubrid installation, this class is named in such a way
 * that Sqoop's default QA process does not run it. You need to run this
 * manually with -Dtestcase=CubridManagerImportTest.
 *
 * You need to put Cubrid JDBC driver library (JDBC-9.2.2.0003-cubrid.jar) in a
 * location where Sqoop will be able to access it (since this library cannot be
 * checked into Apache's tree for licensing reasons).
 *
 * To set up your test environment:
 *   Install Cubrid 9.2.2
 *   ref:http://www.cubrid.org/wiki_tutorials/entry/installing-cubrid-on-linux-using-shell-and-rpm
 *   Create a database SQOOPCUBRIDTEST
 *   $cubrid createdb SQOOPCUBRIDTEST en_us.utf8
 *   Start cubrid and database
 *   $cubrid service start
 *   $cubrid server start SQOOPCUBRIDTEST
 *   Create a login SQOOPUSER with password PASSWORD and grant all
 *   $csql -u dba SQOOPCUBRIDTEST
 *   csql>CREATE USER SQOOPUSER password 'PASSWORD';
 */
public class CubridManagerImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      CubridManagerImportTest.class.getName());

  static final String TABLE_NAME = "employees_cubrid";
  static final String NULL_TABLE_NAME = "null_employees_cubrid";

  // instance variables populated during setUp, used during tests
  private CubridManager manager;

  private Configuration conf = new Configuration();

  @Override
  protected Configuration getConf() {
    return conf;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Before
  public void setUp() {
    super.setUp();
    LOG.debug("Setting up another CubridImport test: "
      + CubridTestUtils.getConnectString());
    setUpData(TABLE_NAME, false);
    setUpData(NULL_TABLE_NAME, true);
    LOG.debug("setUp complete.");
  }

  public void setUpData(String tableName, boolean nullEntry) {
    SqoopOptions options = new SqoopOptions(
        CubridTestUtils.getConnectString(), tableName);
    options.setUsername(CubridTestUtils.getCurrentUser());
    options.setPassword(CubridTestUtils.getPassword());

    LOG.debug("Setting up another CubridImport test: "
      + CubridTestUtils.getConnectString());

    manager = new CubridManager(options);

    Connection connection = null;
    Statement st = null;

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data.
      st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      st.executeUpdate("CREATE TABLE " + tableName + " ("
          + manager.escapeColName("id")
          + " INT NOT NULL PRIMARY KEY, "
          + manager.escapeColName("name")
          + " VARCHAR(24) NOT NULL, "
          + manager.escapeColName("start_date") + " DATE, "
          + manager.escapeColName("Salary") + " FLOAT, "
          + manager.escapeColName("dept")
          + " VARCHAR(32));");

      st.executeUpdate("INSERT INTO " + tableName
          + " VALUES(1,'Aaron','2009-05-14',"
          + "1000000.00,'engineering');");
      st.executeUpdate("INSERT INTO " + tableName
          + " VALUES(2,'Bob','2009-04-20',400.00,'sales');");
      st.executeUpdate("INSERT INTO " + tableName
          + " VALUES(3,'Fred','2009-01-23',"
          + "15.00,'marketing');");
      if (nullEntry) {
        st.executeUpdate("INSERT INTO " + tableName
            + " VALUES(4,'Mike',NULL,NULL,NULL);");
      }

      connection.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    } finally {
      try {
        if (null != st) {
          st.close();
        }

        if (null != connection) {
          connection.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: "
          + sqlE);
      }
    }
  }

  @After
  public void tearDown() {
    super.tearDown();
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Test
  public void testImportSimple() throws IOException {
    String[] expectedResults = {
        "1,Aaron,2009-05-14,1000000.0,engineering",
        "2,Bob,2009-04-20,400.0,sales",
        "3,Fred,2009-01-23,15.0,marketing", };

    doImportAndVerify(TABLE_NAME, expectedResults);
  }

  @Test
  public void testListTables() throws IOException {
    SqoopOptions options = new SqoopOptions(new Configuration());
    options.setConnectString(CubridTestUtils.getConnectString());
    options.setUsername(CubridTestUtils.getCurrentUser());
    options.setPassword(CubridTestUtils.getPassword());

    ConnManager mgr = new CubridManager(options);
    String[] tables = mgr.listTables();
    Arrays.sort(tables);
    assertTrue(TABLE_NAME + " is not found!",
        Arrays.binarySearch(tables, TABLE_NAME) >= 0);
  }

  @Test
  public void testNullEscapeCharacters() throws Exception {
    String[] expectedResults = {
        "1,Aaron,2009-05-14,1000000.0,engineering",
        "2,Bob,2009-04-20,400.0,sales",
        "3,Fred,2009-01-23,15.0,marketing",
        "4,Mike,cubrid,cubrid,cubrid", };

    String[] extraArgs = {
        "--null-string",
        "cubrid",
        "--null-non-string",
        "cubrid", };

    doImportAndVerify(NULL_TABLE_NAME, expectedResults, extraArgs);
  }

  private void doImportAndVerify(String tableName,
      String[] expectedResults,
      String... extraArgs) throws IOException {
    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);
    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String[] argv = getArgv(tableName, extraArgs);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: "
        + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(
          new FileInputStream(f)));
      for (String expectedLine : expectedResults) {
        assertEquals(expectedLine, r.readLine());
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: "
        + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  private String[] getArgv(String tableName, String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CubridTestUtils.getConnectString());
    args.add("--username");
    args.add(CubridTestUtils.getCurrentUser());
    args.add("--password");
    args.add(CubridTestUtils.getPassword());
    args.add("--num-mappers");
    args.add("1");

    if (extraArgs.length > 0) {
      for (String arg : extraArgs) {
        args.add(arg);
      }
    }

    return args.toArray(new String[0]);
  }
}
