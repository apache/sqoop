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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.CubridManager;
import org.apache.sqoop.manager.cubrid.CubridTestUtils;
import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestExport;

/**
 * Test the CubridManager implementation.
 *
 * This uses JDBC to export data from HDFS to an Cubrid database.
 *
 * Since this requires an Cubrid installation, this class is named in such a way
 * that Sqoop's default QA process does not run it. You need to run this
 * manually with -Dtestcase=CubridManagerExportTest.
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
public class CubridManagerExportTest extends TestExport {

  public static final Log LOG = LogFactory.getLog(
      CubridManagerExportTest.class.getName());

  static final String TABLE_PREFIX = "EXPORT_CUBRID_";

  // instance variables populated during setUp, used during tests.
  private CubridManager manager;
  private Connection conn;

  @Override
  protected Connection getConnection() {
    return conn;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String getConnectString() {
    return CubridTestUtils.getConnectString();
  }

  @Override
  protected String getTablePrefix() {
    return TABLE_PREFIX;
  }

  @Override
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE IF EXISTS " + tableName;
  }

  /**
   * Cubrid could not support --staging-table, Diable this test case.
   */
  @Override
  public void testMultiTransactionWithStaging() throws IOException,
      SQLException {
    return;
  }

  /**
   * Cubrid could not support --staging-table, Diable this test case.
   */
  @Override
  public void testMultiMapTextExportWithStaging() throws IOException,
      SQLException {
    return;
  }

  public void createTableAndPopulateData(String table) {
    String fulltableName = manager.escapeTableName(table);

    Statement stmt = null;

    // Drop the existing table, if there is one.
    try {
      conn = manager.getConnection();
      stmt = conn.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + fulltableName);
      conn.commit();
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
      stmt.executeUpdate("CREATE TABLE "
          + fulltableName + " ("
          + "id INT NOT NULL, "
          + "name VARCHAR(24) NOT NULL, "
          + "salary FLOAT, " + "dept VARCHAR(32), "
          + "PRIMARY KEY (id))");
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
        LOG.warn(
            "Exception while closing connection/stmt", ex);
      }
    }
  }

  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(
        CubridTestUtils.getConnectString(),
        getTableName());
    options.setUsername(CubridTestUtils.getCurrentUser());
    options.setPassword(CubridTestUtils.getPassword());
    this.manager = new CubridManager(options);
    try {
      this.conn = manager.getConnection();
      this.conn.setAutoCommit(false);

    } catch (SQLException sqlE) {
      LOG.error(StringUtils.stringifyException(sqlE));
      fail("Failed with sql exception in setup: " + sqlE);
    }
  }

  @After
  public void tearDown() {
    super.tearDown();
    try {
      conn.close();
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Override
  protected String[] getCodeGenArgv(String... extraArgs) {
    String[] moreArgs = new String[extraArgs.length + 4];
    int i = 0;
    for (i = 0; i < extraArgs.length; i++) {
      moreArgs[i] = extraArgs[i];
    }

    // Add username and password args.
    moreArgs[i++] = "--username";
    moreArgs[i++] = CubridTestUtils.getCurrentUser();
    moreArgs[i++] = "--password";
    moreArgs[i++] = CubridTestUtils.getPassword();

    return super.getCodeGenArgv(moreArgs);
  }

  @Override
  protected String[] getArgv(boolean includeHadoopFlags,
      int rowsPerStatement,
      int statementsPerTx, String... additionalArgv) {

    String[] subArgv = newStrArray(additionalArgv, "--username",
        CubridTestUtils.getCurrentUser(), "--password",
        CubridTestUtils.getPassword());
    return super.getArgv(includeHadoopFlags, rowsPerStatement,
        statementsPerTx, subArgv);
  }

  protected void createTestFile(String filename,
      String[] lines)
      throws IOException {
    File testdir = new File(getWarehouseDir());
    if (!testdir.exists()) {
      testdir.mkdirs();
    }
    File file = new File(getWarehouseDir() + "/" + filename);
    Writer output = new BufferedWriter(new FileWriter(file));
    for (String line : lines) {
      output.write(line);
      output.write("\n");
    }
    output.close();
  }

  public static void assertRowCount(long expected,
      String tableName,
      Connection connection) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery("SELECT count(*) FROM "
          + tableName);
      rs.next();
      assertEquals(expected, rs.getLong(1));
    } catch (SQLException e) {
      LOG.error("Can't verify number of rows", e);
      fail();
    } finally {
      try {
        connection.commit();
        if (stmt != null) {
          stmt.close();
        }
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException ex) {
        LOG.info("Ignored exception in finally block.");
      }
    }
  }

  public String escapeTableOrSchemaName(String tableName) {
    return "\"" + tableName + "\"";
  }

  /** Make sure mixed update/insert export work correctly. */
  public void testUpsertTextExport() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;
    createTextFile(0, TOTAL_RECORDS, false);
    createTable();
    // first time will be insert.
    runExport(getArgv(true, 10, 10,
        newStrArray(null, "--update-key", "id",
        "--update-mode", "allowinsert")));
    // second time will be update.
    runExport(getArgv(true, 10, 10,
        newStrArray(null, "--update-key", "id",
        "--update-mode", "allowinsert")));
    verifyExport(TOTAL_RECORDS);
  }
}
