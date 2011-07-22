/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package org.apache.hadoop.sqoop.manager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;

import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.TestExport;
import org.apache.hadoop.sqoop.mapreduce.MySQLExportMapper;

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
    return MySQLTestUtils.CONNECT_STRING;
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

    SqoopOptions options = new SqoopOptions(MySQLTestUtils.CONNECT_STRING,
        getTableName());
    options.setUsername(MySQLTestUtils.getCurrentUser());
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
    super.tearDown();

    if (null != this.conn) {
      try {
        this.conn.close();
      } catch (SQLException sqlE) {
        LOG.error("Got SQLException closing conn: " + sqlE.toString());
      }
    }

    if (null != manager) {
      try {
        manager.close();
        manager = null;
      } catch (SQLException sqlE) {
        LOG.error("Got SQLException: " + sqlE.toString());
        fail("Got SQLException: " + sqlE.toString());
      }
    }
  }

  @Override
  protected String [] getCodeGenArgv(String... extraArgs) {

    String [] moreArgs = new String[extraArgs.length + 2];
    int i = 0;
    for (i = 0; i < extraArgs.length; i++) {
      moreArgs[i] = extraArgs[i];
    }

    // Add username argument for mysql.
    moreArgs[i++] = "--username";
    moreArgs[i++] = MySQLTestUtils.getCurrentUser();

    return super.getCodeGenArgv(moreArgs);
  }

  @Override
  protected String [] getArgv(boolean includeHadoopFlags,
      int rowsPerStatement, int statementsPerTx, String... additionalArgv) {

    String [] subArgv = newStrArray(additionalArgv, "--direct",
        "--username", MySQLTestUtils.getCurrentUser());
    return super.getArgv(includeHadoopFlags, rowsPerStatement,
        statementsPerTx, subArgv);
  }

  /**
   * Test a single mapper that runs several transactions serially.
   */
  public void testMultiTxExport() throws IOException, SQLException {
    multiFileTest(1, 20, 1,
        "-D", MySQLExportMapper.MYSQL_CHECKPOINT_BYTES_KEY + "=10");
  }
}
