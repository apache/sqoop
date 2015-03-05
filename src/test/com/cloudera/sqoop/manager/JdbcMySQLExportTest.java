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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestExport;

/**
 * Test the MySQLManager implementation's exportJob() functionality.
 * This does a better test of ExportOutputFormat than TestExport does,
 * because it supports multi-row INSERT statements.
 */
public class JdbcMySQLExportTest extends TestExport {

  public static final Log LOG = LogFactory.getLog(
      JdbcMySQLExportTest.class.getName());

  static final String TABLE_PREFIX = "EXPORT_MYSQL_J_";

  // instance variables populated during setUp, used during tests.
  private MySQLManager manager;
  private Connection conn;

  @Override
  protected Connection getConnection() {
    return conn;
  }

  // MySQL allows multi-row INSERT statements.
  @Override
  protected int getMaxRowsPerStatement() {
    return 1000;
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
    this.manager = new MySQLManager(options);
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
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(getDropTableStatement(getTableName()));
      stmt.execute(getDropTableStatement(getStagingTableName()));
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

    String [] subArgv = newStrArray(additionalArgv,
        "--username", MySQLTestUtils.getCurrentUser());
    return super.getArgv(includeHadoopFlags, rowsPerStatement,
        statementsPerTx, subArgv);
  }

  public void testIntColInBatchMode() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    // generate a column equivalent to rownum.
    ColumnGenerator gen = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        return "" + rowNum;
      }
      public String getVerifyText(int rowNum) {
        return "" + rowNum;
      }
      public String getType() {
        return "INTEGER";
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, gen);
    createTable(gen);
    runExport(getArgv(true, 10, 10, "--batch"));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), gen);
  }

  public void testUpsert() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    createTextFile(0, TOTAL_RECORDS, false);
    createTable();

    // Insert only
    runExport(getArgv(true, 10, 10, "--update-key", "id",
      "--update-mode", "allowinsert"));
    verifyExport(TOTAL_RECORDS);

    // Update only
    runExport(getArgv(true, 10, 10, "--update-key", "id",
      "--update-mode", "allowinsert"));
    verifyExport(TOTAL_RECORDS);

    // Insert & update
    createTextFile(0, TOTAL_RECORDS * 2, false);
    runExport(getArgv(true, 10, 10, "--update-key", "id",
      "--update-mode", "allowinsert"));
    verifyExport(TOTAL_RECORDS * 2);
  }
}
