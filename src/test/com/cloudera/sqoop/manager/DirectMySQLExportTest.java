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
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestExport;
import com.cloudera.sqoop.mapreduce.MySQLExportMapper;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the DirectMySQLManager implementation's exportJob() functionality.
 */
public class DirectMySQLExportTest extends TestExport {

  public static final Log LOG = LogFactory.getLog(
      DirectMySQLExportTest.class.getName());

  static final String TABLE_PREFIX = "EXPORT_MYSQL_";

  // instance variables populated during setUp, used during tests.
  private DirectMySQLManager manager;
  private Connection conn;
  private MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();

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
    return mySQLTestUtils.getMySqlConnectString();
  }

  @Override
  protected String getTablePrefix() {
    return TABLE_PREFIX;
  }

  @Override
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE IF EXISTS " + tableName;
  }

  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(mySQLTestUtils.getMySqlConnectString(),
        getTableName());
    options.setUsername(mySQLTestUtils.getUserName());
    mySQLTestUtils.addPasswordIfIsSet(options);
    this.manager = new DirectMySQLManager(options);

    try {
      this.conn = manager.getConnection();
      this.conn.setAutoCommit(false);
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    }
  }

  @After
  public void tearDown() {
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(getDropTableStatement(getTableName()));
    } catch(SQLException e) {
      LOG.error("Can't clean up the database:", e);
    }

    super.tearDown();

    if (null != this.conn) {
      try {
        this.conn.close();
      } catch (SQLException sqlE) {
        LOG.error("Got SQLException closing conn: " + sqlE.toString());
      }
    }
  }

  @Override
  protected String [] getCodeGenArgv(String... extraArgs) {
    return super.getCodeGenArgv(mySQLTestUtils.addUserNameAndPasswordToArgs(extraArgs));
  }

  @Override
  protected String [] getArgv(boolean includeHadoopFlags,
      int rowsPerStatement, int statementsPerTx, String... additionalArgv) {

    String [] subArgv = newStrArray(mySQLTestUtils.addUserNameAndPasswordToArgs(additionalArgv),"--direct");
    return super.getArgv(includeHadoopFlags, rowsPerStatement,
        statementsPerTx, subArgv);
  }

  /**
   * Test a single mapper that runs several transactions serially.
   */
  @Test
  public void testMultiTxExport() throws IOException, SQLException {
    multiFileTest(1, 20, 1,
        "-D", MySQLExportMapper.MYSQL_CHECKPOINT_BYTES_KEY + "=10");
  }

  /**
   * Test an authenticated export using mysqlimport.
   */
  @Test
  public void testAuthExport() throws IOException, SQLException {
    SqoopOptions options = new SqoopOptions(MySQLAuthTest.AUTH_CONNECT_STRING,
        getTableName());
    options.setUsername(MySQLAuthTest.AUTH_TEST_USER);
    options.setPassword(MySQLAuthTest.AUTH_TEST_PASS);

    manager = new DirectMySQLManager(options);

    Connection connection = null;
    Statement st = null;

    String tableName = getTableName();

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create a target database table.
      st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      st.executeUpdate("CREATE TABLE " + tableName + " ("
          + "id INT NOT NULL PRIMARY KEY, "
          + "msg VARCHAR(24) NOT NULL)");
      connection.commit();

      // Write a file containing a record to export.
      Path tablePath = getTablePath();
      Path filePath = new Path(tablePath, "datafile");
      Configuration conf = new Configuration();
      conf.set("fs.default.name", "file:///");

      FileSystem fs = FileSystem.get(conf);
      fs.mkdirs(tablePath);
      OutputStream os = fs.create(filePath);
      BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
      w.write(getRecordLine(0));
      w.write(getRecordLine(1));
      w.write(getRecordLine(2));
      w.close();
      os.close();

      // run the export and verify that the results are good.
      runExport(getArgv(true, 10, 10,
          "--username", MySQLAuthTest.AUTH_TEST_USER,
          "--password", MySQLAuthTest.AUTH_TEST_PASS,
          "--connect", MySQLAuthTest.AUTH_CONNECT_STRING));
      verifyExport(3, connection);
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when accessing target table. " + sqlE);
    } finally {
      try {
        if (null != st) {
          st.close();
        }

        if (null != connection) {
          connection.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }
  }

  /**
   * Test an authenticated export using mysqlimport.
   */
  @Test
  public void testEscapedByExport() throws IOException, SQLException {
    SqoopOptions options = new SqoopOptions(MySQLAuthTest.AUTH_CONNECT_STRING,
        getTableName());
    options.setUsername(MySQLAuthTest.AUTH_TEST_USER);
    options.setPassword(MySQLAuthTest.AUTH_TEST_PASS);

    manager = new DirectMySQLManager(options);

    Connection connection = null;
    Statement st = null;

    String tableName = getTableName();

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create a target database table.
      st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      st.executeUpdate("CREATE TABLE " + tableName + " ("
          + "id INT NOT NULL PRIMARY KEY, "
          + "msg VARCHAR(24) NOT NULL, "
          + "value VARCHAR(100) NOT NULL)");
      connection.commit();

      // Write a file containing a record to export.
      Path tablePath = getTablePath();
      Path filePath = new Path(tablePath, "datafile");
      Configuration conf = new Configuration();
      conf.set("fs.default.name", "file:///");

      ColumnGenerator gen = new ColumnGenerator() {
        public String getExportText(int rowNum) {
          return "||" + rowNum;
        }
        public String getVerifyText(int rowNum) {
          return "|" + rowNum;
        }
        public String getType() {
          return "STRING";
        }
      };

      FileSystem fs = FileSystem.get(conf);
      fs.mkdirs(tablePath);
      OutputStream os = fs.create(filePath);
      BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
      w.write(getRecordLine(0, gen));
      w.write(getRecordLine(1, gen));
      w.write(getRecordLine(2, gen));
      w.close();
      os.close();

      // run the export and verify that the results are good.
      runExport(getArgv(true, 10, 10,
          "--username", MySQLAuthTest.AUTH_TEST_USER,
          "--password", MySQLAuthTest.AUTH_TEST_PASS,
          "--connect", MySQLAuthTest.AUTH_CONNECT_STRING,
          "--escaped-by", "|"));
      verifyExport(3, connection);
      verifyTableColumnContents(connection, tableName, "value", gen);
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when accessing target table. " + sqlE);
    } finally {
      try {
        if (null != st) {
          st.close();
        }

        if (null != connection) {
          connection.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }
  }

  @Ignore("Ignoring this test as staging is not supported in direct mode.")
  @Override
  @Test
  public void testMultiMapTextExportWithStaging()
    throws IOException, SQLException {
  }

  @Ignore("Ignoring this test as staging is not supported in direct mode.")
  @Override
  @Test
  public void testMultiTransactionWithStaging()
    throws IOException, SQLException {
  }

  @Ignore("Ignoring this test as --input-null-non-string is not supported in direct mode.")
  @Override
  @Test
  public void testLessColumnsInFileThanInTableInputNullIntPassed() throws IOException, SQLException {
  }

  @Ignore("Ignoring this test as --input-null-string is not supported in direct mode.")
  @Override
  @Test
  public void testLessColumnsInFileThanInTableInputNullStringPassed() throws IOException, SQLException {
  }

  private void verifyTableColumnContents(Connection connection,
    String table, String column, ColumnGenerator gen)
      throws IOException, SQLException {
    Statement st = connection.createStatement();

    // create a target database table.
    assertTrue(st.execute("SELECT " + column + " FROM " + table));
    ResultSet rs = st.getResultSet();

    for (int row = 0; rs.next(); ++row) {
      assertEquals(gen.getVerifyText(row), rs.getString(1));
    }
  }
}
