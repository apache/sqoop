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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.manager.DirectNetezzaManager;
import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestExport;

/**
 * Test the DirectNetezzaManager implementation's exportJob() functionality.
 */
public class DirectNetezzaExportManualTest extends TestExport {

  public static final Log LOG = LogFactory.getLog(
      DirectNetezzaExportManualTest.class.getName());

  static final String TABLE_PREFIX = "EMPNZ";

  // instance variables populated during setUp, used during tests.
  private DirectNetezzaManager manager;
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
    return NetezzaTestUtils.getNZConnectString();
  }

  @Override
  protected String getTablePrefix() {
    return TABLE_PREFIX;
  }

  @Override
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName;
  }

  @Before
  public void setUp() {
    super.setUp();
    conn = getConnection();
    SqoopOptions options = new SqoopOptions(
        NetezzaTestUtils.getNZConnectString(), getTableName());
    options.setUsername(NetezzaTestUtils.getNZUser());
    options.setPassword(NetezzaTestUtils.getNZPassword());
    this.manager = new DirectNetezzaManager(options);

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
    super.tearDown();
    if (null != manager) {
      try {
        manager.close();
      } catch (SQLException sqlE) {
        LOG.error("Got SQLException: " + sqlE.toString());
        fail("Got SQLException: " + sqlE.toString());
      }
    }
    this.conn = null;
    this.manager = null;

  }

  @Override
  protected String [] getCodeGenArgv(String... extraArgs) {

    String [] moreArgs = new String[extraArgs.length + 4];
    int i = 0;
    for (i = 0; i < extraArgs.length; i++) {
      moreArgs[i] = extraArgs[i];
    }

    // Add username argument for netezza.
    moreArgs[i++] = "--username";
    moreArgs[i++] = NetezzaTestUtils.getNZUser();
    moreArgs[i++] = "--password";
    moreArgs[i++] = NetezzaTestUtils.getNZPassword();

    return super.getCodeGenArgv(moreArgs);
  }

  @Override
  protected String [] getArgv(boolean includeHadoopFlags,
      int rowsPerStatement, int statementsPerTx, String... additionalArgv) {

    String [] subArgv = newStrArray(additionalArgv, "--direct",
        "--username", NetezzaTestUtils.getNZUser(), "--password",
        NetezzaTestUtils.getNZPassword());
    return super.getArgv(includeHadoopFlags, rowsPerStatement,
        statementsPerTx, subArgv);
  }



  /**
   * Create the table definition to export to, removing any prior table. By
   * specifying ColumnGenerator arguments, you can add extra columns to the
   * table of arbitrary type.
   */
  @Override
  public void createTable(ColumnGenerator... extraColumns) throws SQLException {
    PreparedStatement statement = conn.prepareStatement(
        getDropTableStatement(getTableName()), ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } catch (SQLException sqle) {
      conn.rollback();
    } finally {
      statement.close();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(getTableName());
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraColumns) {
      sb.append(", " + forIdx(colNum++) + " " + gen.getType());
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
  /**
   * Test an authenticated export using netezza external table import.
   */
  public void testAuthExport() throws IOException, SQLException {
    SqoopOptions options = new SqoopOptions(
        NetezzaTestUtils.getNZConnectString(),
        getTableName());
    options.setUsername(NetezzaTestUtils.getNZUser());
    options.setPassword(NetezzaTestUtils.getNZPassword());

    manager = new DirectNetezzaManager(options);

    Connection connection = null;
    Statement st = null;

    String tableName = getTableName();

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create a target database table.
      try {
        st.executeUpdate("DROP TABLE " + tableName);
      } catch(SQLException sqle) {
        LOG.info("Ignoring exception from DROP TABLE : " + sqle.getMessage());
        connection.rollback();
      }

      LOG.info("Creating table " + tableName);

      st.executeUpdate("CREATE TABLE " + tableName + " ("
          + "id INT NOT NULL PRIMARY KEY, "
          + "msg VARCHAR(24) NOT NULL)");

      connection.commit();
      LOG.info("Created table " + tableName);

      // Write a file containing a record to export.
      Path tablePath = getTablePath();
      Path filePath = new Path(tablePath, "datafile");
      Configuration conf = new Configuration();

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
          "--username", NetezzaTestUtils.getNZUser(),
          "--password", NetezzaTestUtils.getNZPassword(),
          "--connect", NetezzaTestUtils.getNZConnectString()));
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
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }
  }


  @Override
  public void testMultiMapTextExportWithStaging()
    throws IOException, SQLException {
    // disable this test as staging is not supported in direct mode
  }

  @Override
  public void testMultiTransactionWithStaging()
    throws IOException, SQLException {
    // disable this test as staging is not supported in direct mode
  }

  @Override
  public void testColumnsExport()
    throws IOException, SQLException {
    // disable this test as it is not supported in direct mode
  }

  @Override
  public void testSequenceFileExport()
    throws IOException, SQLException {
    // disable this test as it is not supported in direct mode
  }
}
