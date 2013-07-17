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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

/**
 * Test the DB2Manager implementation.
 *
 * This uses JDBC to import data from an DB2 database into HDFS.
 *
 * Since this requires an DB2 Server installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=DB2ManagerImportManualTest
 *
 * You need to put DB2 JDBC driver library (db2jcc4.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * To set up your test environment:
 *   Install DB2 Express 9.7 C server.
 *   Create a database SQOOP
 *   Create a login SQOOP with password PASSWORD and grant all
 *   access for database SQOOP to user SQOOP.
 */
public class DB2ManagerImportManualTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
        DB2ManagerImportManualTest.class.getName());

  static final String HOST_URL = System.getProperty(
          "sqoop.test.db2.connectstring.host_url",
          "jdbc:db2://db2host:50000");

  static final String DATABASE_NAME = System.getProperty(
          "sqoop.test.db2.connectstring.database",
          "SQOOP");
  static final String DATABASE_USER = System.getProperty(
          "sqoop.test.db2.connectstring.username",
          "SQOOP");
  static final String DATABASE_PASSWORD = System.getProperty(
          "sqoop.test.db2.connectstring.password",
          "SQOOP");
  static final String TABLE_NAME = "EMPLOYEES_DB2";
  static final String QUALIFIED_TABLE_NAME = DATABASE_USER + ".EMPLOYEES_DB2";
  static final String CONNECT_STRING = HOST_URL
              + "/" + DATABASE_NAME
              + ":currentSchema=" + DATABASE_USER +";";

  static {
    LOG.info("Using DB2 CONNECT_STRING: " + CONNECT_STRING);
  }

  // instance variables populated during setUp, used during tests
  private Db2Manager manager;
  private boolean useQualifiedTableName;
  private boolean useDefaultConnectManager;

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String getTableName() {
    return useQualifiedTableName ? QUALIFIED_TABLE_NAME : TABLE_NAME;
  }

  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(CONNECT_STRING, getTableName());
    options.setUsername(DATABASE_USER);
    options.setPassword(DATABASE_PASSWORD);

    manager = new Db2Manager(options);

    // Drop the existing table, if there is one.
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = manager.getConnection();
      stmt = conn.createStatement();
      stmt.execute("DROP TABLE " + getTableName());
    } catch (SQLException sqlE) {
      LOG.info("Table was not dropped: " + sqlE.getMessage());
    } finally {
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (Exception ex) {
        LOG.warn("Exception while closing stmt", ex);
      }
    }

    // Create and populate table
    try {
      conn = manager.getConnection();
      conn.setAutoCommit(false);
      stmt = conn.createStatement();

      // create the database table and populate it with data.
      stmt.executeUpdate("CREATE TABLE " + getTableName() + " ("
          + "id INT NOT NULL, "
          + "name VARCHAR(24) NOT NULL, "
          + "salary FLOAT, "
          + "dept VARCHAR(32), "
          + "PRIMARY KEY (id))");

      stmt.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "1,'Aaron', "
          + "1000000.00,'engineering')");
      stmt.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "2,'Bob', "
          + "400.00,'sales')");
      stmt.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "3,'Fred', 15.00,"
          + "'marketing')");
      conn.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: ", sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    } finally {
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (Exception ex) {
        LOG.warn("Exception while closing connection/stmt", ex);
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
  public void testDb2Import() throws IOException {
    useQualifiedTableName = false;

    // Verify that GenericJdbcManager works.
    useDefaultConnectManager = true;
    runDb2Test(getExpectedResults());

    // Verify that Db2Manager works.
    useDefaultConnectManager = false;
    runDb2Test(getExpectedResults());
  }

  @Test
  public void testDb2ImportQualifiedTableName() throws IOException {
    useQualifiedTableName = true;

    // Verify that GenericJdbcManager works.
    useDefaultConnectManager = true;
    runDb2Test(getExpectedResults());

    // Verify that Db2Manager works.
    useDefaultConnectManager = false;
    runDb2Test(getExpectedResults());
  }

  private String [] getExpectedResults() {
    return new String [] {
      "1,Aaron,1000000.0,engineering",
      "2,Bob,400.0,sales",
      "3,Fred,15.0,marketing",
    };
  }

  private String [] getArgv() {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--username");
    args.add(DATABASE_USER);
    args.add("--password");
    args.add(DATABASE_PASSWORD);
    args.add("--num-mappers");
    args.add("1");

    if (useDefaultConnectManager) {
      // Specifying the driver class forces DefaultManagerFactory
      // to instantiate GenericJdbcManager.
      args.add("--driver");
      args.add("com.ibm.db2.jcc.DB2Driver");
    }

    return args.toArray(new String[0]);
  }

  private void runDb2Test(String [] expectedResults) throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, getTableName());
    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv();
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      for (String expectedLine : expectedResults) {
        assertEquals(expectedLine, r.readLine());
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

}
