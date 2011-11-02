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

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.ExportTool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * Tests importing a database table as an Avro Data File then back to the
 * database.
 */
public class TestAvroImportExportRoundtrip extends ImportJobTestCase {

  public static final Log LOG = LogFactory
      .getLog(TestAvroImportExportRoundtrip.class.getName());

  public void testRoundtripQuery() throws IOException, SQLException {
    String[] argv = {};

    runImport(getOutputArgvForQuery(true));
    deleteTableData();
    runExport(getExportArgvForQuery(true, 10, 10, newStrArray(argv, "-m",
        "" + 1)));

    checkFirstColumnSum();
  }

  public void testRoundtrip() throws IOException, SQLException {
    String[] argv = {};

    runImport(getOutputArgv(true));
    deleteTableData();
    runExport(getExportArgv(true, 10, 10, newStrArray(argv, "-m", "" + 1)));

    checkFirstColumnSum();
  }

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected String[] getOutputArgv(boolean includeHadoopFlags) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(HsqldbTestServer.getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--as-avrodatafile");

    return args.toArray(new String[0]);
  }

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected String[] getOutputArgvForQuery(boolean includeHadoopFlags) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--query");
    args.add("select * from " + HsqldbTestServer.getTableName()
        + " where $CONDITIONS");
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--target-dir");
    args.add(getWarehouseDir() + "/query_result");
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--as-avrodatafile");

    return args.toArray(new String[0]);
  }

  protected String [] getExportArgv(boolean includeHadoopFlags,
      int rowsPerStmt, int statementsPerTx, String... additionalArgv) {
    ArrayList<String> args = formatAdditionalArgs(additionalArgv);

    args.add("--table");
    args.add(getTableName());
    args.add("--export-dir");
    args.add(getTablePath().toString());
    args.add("--connect");
    args.add(getConnectString());
    args.add("-m");
    args.add("1");

    LOG.debug("args:");
    for (String a : args) {
      LOG.debug("  " + a);
    }

    return args.toArray(new String[0]);
  }

  protected String [] getExportArgvForQuery(boolean includeHadoopFlags,
      int rowsPerStmt, int statementsPerTx, String... additionalArgv) {
    ArrayList<String> args = formatAdditionalArgs(additionalArgv);

    args.add("--table");
    args.add(getTableName());
    args.add("--export-dir");
    args.add(getWarehouseDir() + "/query_result");
    args.add("--connect");
    args.add(getConnectString());
    args.add("-m");
    args.add("1");

    LOG.debug("args:");
    for (String a : args) {
      LOG.debug("  " + a);
    }

    return args.toArray(new String[0]);
  }

  /**
   * Create the argv to pass to Sqoop.
   * @param includeHadoopFlags if true, then include -D various.settings=values
   * @param rowsPerStmt number of rows to export in a single INSERT statement.
   * @param statementsPerTx ## of statements to use in a transaction.
   * @return the argv as an array of strings.
   */
  protected ArrayList<String> formatAdditionalArgs(String... additionalArgv) {
    ArrayList<String> args = new ArrayList<String>();

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
          args.add(arg);
        }
      }
    }
    return args;
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }

  private void deleteTableData() throws SQLException {
    Connection conn = getConnection();
    PreparedStatement statement = conn.prepareStatement(
        "DELETE FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
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
      Sqoop sqoop = new Sqoop(exporter);
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

  private void checkFirstColumnSum() throws SQLException {
    Connection conn = getConnection();

    PreparedStatement statement = conn.prepareStatement(
        "SELECT SUM(INTFIELD1) FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    int actualVal = 0;
    try {
      ResultSet rs = statement.executeQuery();
      try {
        rs.next();
        actualVal = rs.getInt(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    assertEquals("First column column sum", HsqldbTestServer.getFirstColSum(),
        actualVal);
  }
}
