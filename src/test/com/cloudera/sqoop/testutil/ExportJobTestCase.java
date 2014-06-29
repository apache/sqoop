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

package com.cloudera.sqoop.testutil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.mapreduce.ExportOutputFormat;
import com.cloudera.sqoop.tool.ExportTool;

/**
 * Class that implements common methods required for tests which export data
 * from HDFS to databases, to verify correct export.
 */
public abstract class ExportJobTestCase extends BaseSqoopTestCase {

  public static final Log LOG = LogFactory.getLog(
      ExportJobTestCase.class.getName());

  @Before
  public void setUp() {
    // start the server
    super.setUp();

    if (useHsqldbTestServer()) {
      // throw away any existing data that might be in the database.
      try {
        this.getTestServer().dropExistingSchema();
      } catch (SQLException sqlE) {
        fail(sqlE.toString());
      }
    }
  }

  protected String getTablePrefix() {
    return "EXPORT_TABLE_";
  }

  /**
   * @return the maximum rows to fold into an INSERT statement.
   * HSQLDB can only support the single-row INSERT syntax. Other databases
   * can support greater numbers of rows per statement.
   */
  protected int getMaxRowsPerStatement() {
    return 1;
  }

  /**
   * Create the argv to pass to Sqoop.
   * @param includeHadoopFlags if true, then include -D various.settings=values
   * @param rowsPerStmt number of rows to export in a single INSERT statement.
   * @param statementsPerTx ## of statements to use in a transaction.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags,
      int rowsPerStmt, int statementsPerTx, String... additionalArgv) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
      args.add("-D");
      int realRowsPerStmt = Math.min(rowsPerStmt, getMaxRowsPerStatement());
      if (realRowsPerStmt != rowsPerStmt) {
        LOG.warn("Rows per statement set to " + realRowsPerStmt
            + " by getMaxRowsPerStatement() limit.");
      }
      args.add(ExportOutputFormat.RECORDS_PER_STATEMENT_KEY + "="
          + realRowsPerStmt);
      args.add("-D");
      args.add(ExportOutputFormat.STATEMENTS_PER_TRANSACTION_KEY + "="
          + statementsPerTx);
    }

    // Any additional Hadoop flags (-D foo=bar) are prepended.
    if (null != additionalArgv) {
      boolean prevIsFlag = false;
      for (String arg : additionalArgv) {
        if (arg.equals("-D")) {
          args.add(arg);
          prevIsFlag = true;
        } else if (prevIsFlag) {
          args.add(arg);
          prevIsFlag = false;
        }
      }
    }
    boolean isHCatJob = false;
    // The sqoop-specific additional args are then added.
    if (null != additionalArgv) {
      boolean prevIsFlag = false;
      for (String arg : additionalArgv) {
        if (arg.equals("-D")) {
          prevIsFlag = true;
          continue;
        } else if (prevIsFlag) {
          prevIsFlag = false;
          continue;
        } else {
          // normal argument.
          if (!isHCatJob && arg.equals("--hcatalog-table")) {
            isHCatJob = true;
          }
          args.add(arg);
        }
      }
    }

    if (usesSQLtable()) {
      args.add("--table");
      args.add(getTableName());
    }
    // Only add export-dir if hcatalog-table is not there in additional argv
    if (!isHCatJob) {
      args.add("--export-dir");
      args.add(getTablePath().toString());
    }
    args.add("--connect");
    args.add(getConnectString());
    args.add("--fields-terminated-by");
    args.add("\\t");
    args.add("--lines-terminated-by");
    args.add("\\n");
    args.add("-m");
    args.add("1");

    LOG.debug("args:");
    for (String a : args) {
      LOG.debug("  " + a);
    }

    return args.toArray(new String[0]);
  }

  protected boolean usesSQLtable() {
    return true;
  }

  /** When exporting text columns, what should the text contain? */
  protected String getMsgPrefix() {
    return "textfield";
  }


  /** @return the minimum 'id' value in the table */
  protected int getMinRowId(Connection conn) throws SQLException {
    PreparedStatement statement = conn.prepareStatement(
        "SELECT MIN(id) FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    int minVal = 0;
    try {
      ResultSet rs = statement.executeQuery();
      try {
        rs.next();
        minVal = rs.getInt(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    return minVal;
  }

  /** @return the maximum 'id' value in the table */
  protected int getMaxRowId(Connection conn) throws SQLException {
    PreparedStatement statement = conn.prepareStatement(
        "SELECT MAX(id) FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    int maxVal = 0;
    try {
      ResultSet rs = statement.executeQuery();
      try {
        rs.next();
        maxVal = rs.getInt(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    return maxVal;
  }

  /**
   * Check that we got back the expected row set.
   * @param expectedNumRecords The number of records we expected to load
   * into the database.
   */
  protected void verifyExport(int expectedNumRecords)
      throws IOException, SQLException {
    Connection conn = getConnection();
    verifyExport(expectedNumRecords, conn);
  }

  /**
   * Check that we got back the expected row set.
   * @param expectedNumRecords The number of records we expected to load
   * into the database.
   * @param conn the db connection to use.
   */
  protected void verifyExport(int expectedNumRecords, Connection conn)
      throws IOException, SQLException {
    LOG.info("Verifying export: " + getTableName());
    // Check that we got back the correct number of records.
    PreparedStatement statement = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    int actualNumRecords = 0;
    ResultSet rs = null;
    try {
      rs = statement.executeQuery();
      try {
        rs.next();
        actualNumRecords = rs.getInt(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    assertEquals("Got back unexpected row count", expectedNumRecords,
        actualNumRecords);

    if (expectedNumRecords == 0) {
      return; // Nothing more to verify.
    }

    // Check that we start with row 0.
    int minVal = getMinRowId(conn);
    assertEquals("Minimum row was not zero", 0, minVal);

    // Check that the last row we loaded is numRows - 1
    int maxVal = getMaxRowId(conn);
    assertEquals("Maximum row had invalid id", expectedNumRecords - 1, maxVal);

    // Check that the string values associated with these points match up.
    statement = conn.prepareStatement("SELECT msg FROM " + getTableName()
        + " WHERE id = " + minVal,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    String minMsg = "";
    try {
      rs = statement.executeQuery();
      try {
        rs.next();
        minMsg = rs.getString(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    assertEquals("Invalid msg field for min value", getMsgPrefix() + minVal,
        minMsg);

    statement = conn.prepareStatement("SELECT msg FROM " + getTableName()
        + " WHERE id = " + maxVal,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    String maxMsg = "";
    try {
      rs = statement.executeQuery();
      try {
        rs.next();
        maxMsg = rs.getString(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    assertEquals("Invalid msg field for min value", getMsgPrefix() + maxVal,
        maxMsg);
  }

  /**
   * Run a MapReduce-based export (using the argv provided to control
   * execution).
   * @return the generated jar filename
   */
  protected List<String> runExport(String [] argv) throws IOException {
    // run the tool through the normal entry-point.
    int ret;
    List<String> generatedJars = null;
    try {
      ExportTool exporter = new ExportTool();
      Configuration conf = getConf();
      //Need to disable OraOop for existing tests
      conf.set("oraoop.disabled", "true");
      SqoopOptions opts = getSqoopOptions(conf);
      Sqoop sqoop = new Sqoop(exporter, conf, opts);
      ret = Sqoop.runSqoop(sqoop, argv);
      generatedJars = exporter.getGeneratedJarFiles();
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: "
          + StringUtils.stringifyException(e));
      ret = 1;
    }

    // expect a successful return.
    if (0 != ret) {
      throw new IOException("Failure during job; return status " + ret);
    }

    return generatedJars;
  }

}
