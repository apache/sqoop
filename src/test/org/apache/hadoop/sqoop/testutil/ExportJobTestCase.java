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

package org.apache.hadoop.sqoop.testutil;

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

import org.apache.hadoop.sqoop.Sqoop;
import org.apache.hadoop.sqoop.tool.ExportTool;

/**
 * Class that implements common methods required for tests which export data
 * from HDFS to databases, to verify correct export
 */
public class ExportJobTestCase extends BaseSqoopTestCase {

  public static final Log LOG = LogFactory.getLog(ExportJobTestCase.class.getName());

  protected String getTablePrefix() {
    return "EXPORT_TABLE_";
  }

  /**
   * @return a connection to the database under test.
   */
  protected Connection getConnection() {
    try {
      return getTestServer().getConnection();
    } catch (SQLException sqlE) {
      LOG.error("Could not get connection to test server: " + sqlE);
      return null;
    }
  }

  /**
   * Create the argv to pass to Sqoop
   * @param includeHadoopFlags if true, then include -D various.settings=values
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags, String... additionalArgv) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
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

    args.add("--table");
    args.add(getTableName());
    args.add("--export-dir");
    args.add(getTablePath().toString());
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

  /** When exporting text columns, what should the text contain? */
  protected String getMsgPrefix() {
    return "textfield";
  }


  /** @return the minimum 'id' value in the table */
  protected int getMinRowId() throws SQLException {
    Connection conn = getConnection();
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
  protected int getMaxRowId() throws SQLException {
    Connection conn = getConnection();
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
   * Check that we got back the expected row set
   * @param expectedNumRecords The number of records we expected to load
   * into the database.
   */
  protected void verifyExport(int expectedNumRecords) throws IOException, SQLException {
    Connection conn = getConnection();

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

    // Check that we start with row 0.
    int minVal = getMinRowId();
    assertEquals("Minimum row was not zero", 0, minVal);

    // Check that the last row we loaded is numRows - 1
    int maxVal = getMaxRowId();
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

    assertEquals("Invalid msg field for min value", getMsgPrefix() + minVal, minMsg);

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

    assertEquals("Invalid msg field for min value", getMsgPrefix() + maxVal, maxMsg);
  }

  /**
   * Run a MapReduce-based export (using the argv provided to control execution).
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

}
