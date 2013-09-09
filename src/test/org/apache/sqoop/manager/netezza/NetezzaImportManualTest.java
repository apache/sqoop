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
package org.apache.sqoop.manager.netezza;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.sqoop.manager.NetezzaManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

/**
 * Test the Netezza implementation.
 *
 * This uses both JDBC and external tables to import data from an Netezza
 * database into HDFS.
 *
 * Since this requires an Netezza Server installation, this class is named in
 * such a way that Sqoop's default QA process does not run it. You need to run
 * this manually with -Dtestcase=NetezzaImportManualTest.
 *
 */
public class NetezzaImportManualTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.
      getLog(NetezzaImportManualTest.class.getName());

  // instance variables populated during setUp, used during tests
  private NetezzaManager manager;
  private Connection conn;
  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String getTableName() {
    return NetezzaTestUtils.TABLE_NAME + "_IMP_";
  }


  private void createTable(String tableName, String... extraColumns)
      throws SQLException {
    PreparedStatement statement = conn.prepareStatement("DROP TABLE "
        + tableName, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } catch (SQLException sqle) {
      conn.rollback();
    } finally {
      statement.close();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE " + tableName + " (");
    sb.append("id INT NOT NULL PRIMARY KEY, ");
    sb.append("name VARCHAR(24) NOT NULL, ");
    sb.append("start_date DATE, ");
    sb.append("Salary FLOAT, ");
    sb.append("Fired BOOL, ");
    sb.append("dept VARCHAR(32) ");
    for (String col : extraColumns) {
      sb.append(", " + col + "  INTEGER");
    }
    sb.append(")");

    statement = conn.prepareStatement(sb.toString(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  private void populateTable(String tableName) throws SQLException {
    Statement statement = conn.createStatement();
    try {
      statement.executeUpdate("INSERT INTO " + tableName
          + " VALUES(1,'Aaron','2009-05-14',1000000.00,TRUE,'engineering')");
      statement.executeUpdate("INSERT INTO " + tableName
          + " VALUES(2,'Bob','2009-04-20',400.00,TRUE,'sales')");
      statement.executeUpdate("INSERT INTO " + tableName
          + " VALUES(3,'Fred','2009-01-23',15.00,FALSE,'marketing')");
      conn.commit();
    } finally {
      statement.close();
    }
  }

  private void populateTableWithNull(String tableName) throws SQLException{
    Statement statement = conn.createStatement();
    try {
      statement.executeUpdate("INSERT INTO " + tableName
          + " VALUES(1,'Aaron','2009-05-14',1000000.00,TRUE,"
          + "'engineering',NULL,1)");
      statement.executeUpdate("INSERT INTO " + tableName
          + " VALUES(2,'Bob','2009-04-20',400.00,TRUE,'sales',NULL,2)");
      statement.executeUpdate("INSERT INTO " + tableName
          + " VALUES(3,'Fred','2009-01-23',15.00,FALSE,'marketing',NULL,3)");
      conn.commit();
    } finally {
      statement.close();
    }
  }

  public void setUpData() {
    SqoopOptions options = new SqoopOptions(
        NetezzaTestUtils.getNZConnectString(), getTableName());
    options.setUsername(NetezzaTestUtils.getNZUser());
    options.setPassword(NetezzaTestUtils.getNZPassword());
    try {
      manager = new NetezzaManager(options);
      conn = manager.getConnection();
      createTable(getTableName());
      populateTable(getTableName());
      String tableNameWithNull = getTableName() + "_W_N";
      createTable(tableNameWithNull, new String[] { "col0", "col1" });
      populateTableWithNull(tableNameWithNull);
    } catch (SQLException sqlE) {
      fail("Setup failed with SQLException " + sqlE);
    }
  }

  @Before
  public void setUp() {
    super.setUp();
    setUpData();
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

  private String[] getExpectedResults() {
    String [] expectedResults = {
        "1,Aaron,2009-05-14,1000000.0,true,engineering",
        "2,Bob,2009-04-20,400.0,true,sales",
        "3,Fred,2009-01-23,15.0,false,marketing",
      };

    return expectedResults;
  }
  private String[] getDirectModeExpectedResults() {
    String [] expectedResults = {
        "1,Aaron,2009-05-14,1000000,T,engineering",
        "2,Bob,2009-04-20,400,T,sales",
        "3,Fred,2009-01-23,15,F,marketing",
      };
    return expectedResults;
  }
  private String[] getExpectedResultsWithNulls() {
    String [] expectedResults = {
        "1,Aaron,2009-05-14,1000000.0,true,engineering,\\N,1",
        "2,Bob,2009-04-20,400.0,true,sales,\\N,2",
        "3,Fred,2009-01-23,15.0,false,marketing,\\N,3",
      };

    return expectedResults;
  }

  private String[] getDirectModeExpectedResultsWithNulls() {
    String [] expectedResults = {
        "1,Aaron,2009-05-14,1000000,T,engineering,\\N,1",
        "2,Bob,2009-04-20,400,T,sales,\\N,2",
        "3,Fred,2009-01-23,15,F,marketing,\\N,3",
      };

    return expectedResults;
  }

  private String[] getArgv(boolean isDirect, String tableName,
      String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(NetezzaTestUtils.getNZConnectString());
    args.add("--username");
    args.add(NetezzaTestUtils.getNZUser());
    args.add("--password");
    args.add(NetezzaTestUtils.getNZPassword());
    args.add("--num-mappers");
    args.add("1");

    if (isDirect) {
      args.add("--direct");
    }
    for (String arg : extraArgs) {
      args.add(arg);
    }
    return args.toArray(new String[args.size()]);
  }

  private void runNetezzaTest(boolean isDirect, String tableName,
      String[] expectedResults, String... extraArgs) throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);

    Path filePath;

    filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String[] argv = getArgv(isDirect, tableName, extraArgs);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file : " + f, f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      String[] s = new String[3];
      for (int i = 0; i < s.length; ++i) {
        s[i] = r.readLine();
        LOG.info("Line read from file = " + s[i]);
      }
      Arrays.sort(s);
      for (int i = 0; i < expectedResults.length; ++i) {
        assertEquals(expectedResults[i], s[i]);
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  @Test
  public void testNetezzaImport() throws IOException {

    runNetezzaTest(false, getTableName(), getExpectedResults());
  }

  @Test
  public void testDirectImport() throws IOException {
    runNetezzaTest(true, getTableName(), getDirectModeExpectedResults());
  }

  @Test
  public void testListTables() throws IOException {
    SqoopOptions options = new SqoopOptions(
        NetezzaTestUtils.getNZConnectString(), getTableName());
    options.setUsername(NetezzaTestUtils.getNZUser());
    options.setPassword(NetezzaTestUtils.getNZPassword());

    ConnManager mgr = new NetezzaManager(options);
    String[] tables = mgr.listTables();
    Arrays.sort(tables);
    assertTrue(getTableName() + " is not found!",
        Arrays.binarySearch(tables, getTableName()) >= 0);
  }

  @Test
  public void testIncrementalImport() throws IOException {
    String[] expectedResults = {};

    String[] extraArgs = { "--incremental", "lastmodified", "--check-column",
        "START_DATE", };

    runNetezzaTest(false, getTableName(), expectedResults, extraArgs);
  }

  @Test
  public void testNullStringValue() throws Exception {


     String [] extraArgs = {
         "--null-string", "\\\\N",
         "--null-non-string", "\\\\N",
      };

     String[] expectedResultsWithNulls =
       getExpectedResultsWithNulls();
     String tableNameWithNull = getTableName() + "_W_N";

     runNetezzaTest(false, tableNameWithNull, expectedResultsWithNulls,
        extraArgs);
  }

  @Test
  public void testValidExtraArgs() throws Exception {

    String [] extraArgs = {
        "--",
        "--log-dir", "/tmp",
        "--max-errors", "2",
     };
    String[] expectedResults = getDirectModeExpectedResults();
    String tableName = getTableName();

    runNetezzaTest(true, tableName, expectedResults,
       extraArgs);
  }

}
