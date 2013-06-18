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

package org.apache.sqoop.manager.sqlserver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.orm.CompilationManager;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.testutil.SeqFileReader;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

/**
 * Test that --split-by works.
 */
public class SQLServerSplitByManualTest extends ImportJobTestCase {

  public void setUp() {
    super.setUp();
    MSSQLTestUtils utils = new MSSQLTestUtils();
    try {
      utils.createTableFromSQL(MSSQLTestUtils.CREATE_TALBE_LINEITEM);
      utils.populateLineItem();
    } catch (SQLException e) {
      LOG.error("Setup fail with SQLException: " + StringUtils.stringifyException(e));
      fail("Setup fail with SQLException: " + e.toString());
    }

  }

  public void tearDown() {
    super.tearDown();
    MSSQLTestUtils utils = new MSSQLTestUtils();
    try {
      utils.dropTableIfExists("TPCH1M_LINEITEM");
    } catch (SQLException e) {
      LOG.error("TearDown fail with SQLException: " + StringUtils.stringifyException(e));
      fail("TearDown fail with SQLException: " + e.toString());
    }
  }

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected String[] getArgv(boolean includeHadoopFlags, String[] colNames,
      String splitByCol) {
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add("tpch1m_lineitem");
    args.add("--columns");
    args.add(columnsString);
    args.add("--split-by");
    args.add("L_ORDERKEY");
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  /**
   * Given a comma-delimited list of integers, grab and parse the first int.
   *
   * @param str
   *            a comma-delimited list of values, the first of which is an
   *            int.
   * @return the first field in the string, cast to int
   */
  private int getFirstInt(String str) {
    String[] parts = str.split(",");
    return Integer.parseInt(parts[0]);
  }

  public void runSplitByTest(String splitByCol, int expectedSum)
      throws IOException {

    String[] columns = new String[] { "L_ORDERKEY", "L_PARTKEY",
        "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE",
        "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS",
        "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE",
        "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT", };
    ClassLoader prevClassLoader = null;
    SequenceFile.Reader reader = null;

    String[] argv = getArgv(true, columns, splitByCol);
    runImport(argv);
    try {
      SqoopOptions opts = new ImportTool().parseArguments(getArgv(false,
          columns, splitByCol), null, null, true);

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();
      LOG.debug("Got jar from import job: " + jarFileName);

      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
          getTableName());

      reader = SeqFileReader.getSeqFileReader(getDataFilePath()
          .toString());

      // here we can actually instantiate (k, v) pairs.
      Configuration conf = new Configuration();
      Object key = ReflectionUtils
          .newInstance(reader.getKeyClass(), conf);
      Object val = ReflectionUtils.newInstance(reader.getValueClass(),
          conf);

      // We know that these values are two ints separated by a ','
      // character.
      // Since this is all dynamic, though, we don't want to actually link
      // against the class and use its methods. So we just parse this back
      // into int fields manually. Sum them up and ensure that we get the
      // expected total for the first column, to verify that we got all
      // the
      // results from the db into the file.

      // Sum up everything in the file.
      int curSum = 0;
      while (reader.next(key) != null) {
        reader.getCurrentValue(val);
        curSum += getFirstInt(val.toString());
      }
      System.out.println("Sum : e,c" + expectedSum + " : " + curSum);
      assertEquals("Total sum of first db column mismatch", expectedSum,
          curSum);
    } catch (InvalidOptionsException ioe) {
      LOG.error(StringUtils.stringifyException(ioe));
      fail(ioe.toString());
    } catch (ParseException pe) {
      LOG.error(StringUtils.stringifyException(pe));
      fail(pe.toString());
    } finally {
      IOUtils.closeStream(reader);

      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void testSplitByFirstCol() throws IOException {
    String splitByCol = "L_ORDERKEY";
    runSplitByTest(splitByCol, 10);
  }

  public void testSplitBySecondCol() throws IOException {
    String splitByCol = "L_PARTKEY";
    runSplitByTest(splitByCol, 10);
  }

  protected boolean useHsqldbTestServer() {

    return false;
  }

  protected String getConnectString() {
    return System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");
  }

  protected String getTableName() {
    return "tpch1m_lineitem";
  }

  /**
   * Drop a table if it already exists in the database.
   *
   * @param table
   *            the name of the table to drop.
   * @throws SQLException
   *             if something goes wrong.
   */
  protected void dropTableIfExists(String table) throws SQLException {
    Connection conn = getManager().getConnection();
    String sqlStmt = "IF OBJECT_ID('" + table
        + "') IS NOT NULL  DROP TABLE " + table;
    System.out.println("@abhi SQL for drop :" + sqlStmt);
    PreparedStatement statement = conn.prepareStatement(sqlStmt,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();
    opts.setUsername(username);
    opts.setPassword(password);
    return opts;

  }
}
