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

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestData.KEY_STRINGS;
import org.apache.sqoop.manager.sqlserver.MSSQLTestDataFileParser.DATATYPES;
import org.junit.Before;
import org.junit.Test;
import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.ExportJobTestCase;
import com.cloudera.sqoop.tool.ExportTool;

/**
 * Test utilities for export to SQL Server.
*/
public abstract class ManagerCompatExport extends ExportJobTestCase {

  private MSSQLTestDataFileParser tdfs;

  public void createTable(DATATYPES dt) throws SQLException {
    String tname = getTableName(dt);
    String createTableSql = "CREATE TABLE " + tname + " ( " + getColName()
        + " " + dt.toString() + " )";

    dropTableIfExists(tname);

    Connection conn = getManager().getConnection();
    PreparedStatement statement = conn.prepareStatement(createTableSql,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.executeUpdate();
    conn.commit();
    statement.close();

  }

  public void createTable(DATATYPES dt, MSSQLTestData td) throws Exception {
    String tname = getTableName(dt);
    String scale = td.getData(KEY_STRINGS.SCALE);
    String precision = td.getData(KEY_STRINGS.PREC);
    String createTableSql = "";
    if (scale != null && precision != null) {
      // this is decimal/numeric thing
      createTableSql = "CREATE TABLE " + tname + " (" + getColName()
          + " " + dt.toString() + "(" + scale + "," + precision
          + ") )";
    } else if (scale != null && precision == null) {
      // this is decimal/numeric thing
      createTableSql = "CREATE TABLE " + tname + " ( " + getColName()
          + " " + dt.toString() + "(" + scale + ") )";
    } else {
      throw new Exception("Invalid data for create table");
    }

    dropTableIfExists(tname);

    Connection conn = getManager().getConnection();
    PreparedStatement statement = conn.prepareStatement(createTableSql,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.executeUpdate();
    conn.commit();
    statement.close();

  }

  public String getColName() {
    return "COL_1";
  }

  public String getTableName(DATATYPES dt) {
    return "t_" + dt.toString();
  }

  public Path getTablePath(DATATYPES dt) {
    Path warehousePath = new Path(getWarehouseDir());
    Path tablePath = new Path(warehousePath, getTableName(dt));
    return tablePath;

  }

  public abstract void createFile(DATATYPES dt, String[] data)
      throws Exception;

  public abstract void createFile(DATATYPES dt, String data) throws Exception;

  public void testVarBinary() {

    exportTestMethod(DATATYPES.VARBINARY);

  }

  public void testTime() {

    exportTestMethod(DATATYPES.TIME);

  }

  @Test
  public void testSmalldatetime() {

    exportTestMethod(DATATYPES.SMALLDATETIME);

  }

  @Test
  public void testdatetime2() {

    exportTestMethod(DATATYPES.DATETIME2);

  }

  @Test
  public void testdatetime() {

    exportTestMethod(DATATYPES.DATETIME);

  }

  @Test
  public void testdatetimeoffset() {

    exportTestMethod(DATATYPES.DATETIMEOFFSET);

  }

  public void testDecimal() {
    exportTestMethod(DATATYPES.DECIMAL);

  }

  public void testNumeric() {
    exportTestMethod(DATATYPES.NUMERIC);

  }

  @Test
  public void testBigInt() {

    exportTestMethod(DATATYPES.BIGINT);
  }

  @Test
  public void testInt() {
    exportTestMethod(DATATYPES.INT);

  }

  @Test
  public void testSmallInt() {
    exportTestMethod(DATATYPES.SMALLINT);

  }

  @Test
  public void testTinyint() {
    exportTestMethod(DATATYPES.TINYINT);

  }

  @Test
  public void testFloat() {
    exportTestMethod(DATATYPES.FLOAT);

  }

  @Test
  public void testReal() {
    exportTestMethod(DATATYPES.REAL);

  }

  @Test
  public void testDate() {
    exportTestMethod(DATATYPES.DATE);

  }

  public void testMoney() {
    exportTestMethod(DATATYPES.MONEY);

  }

  @Test
  public void testSmallMoney() {
    exportTestMethod(DATATYPES.SMALLMONEY);

  }

  @Test
  public void testText() {
    exportTestMethod(DATATYPES.TEXT);

  }

  @Test
  public void testVarchar() {
    exportTestMethod(DATATYPES.VARCHAR);

  }

  @Test
  public void testChar() {
    exportTestMethod(DATATYPES.CHAR);

  }

  @Test
  public void testNText() {
    exportTestMethod(DATATYPES.NTEXT);

  }

  @Test
  public void testNChar() {
    exportTestMethod(DATATYPES.NCHAR);

  }

  @Test
  public void testNVarchar() {
    exportTestMethod(DATATYPES.NVARCHAR);

  }

  public void testImage() {
    exportTestMethod(DATATYPES.IMAGE);

  }

  public void testBinary() {
    exportTestMethod(DATATYPES.BINARY);

  }

  public void exportTestMethod(DATATYPES dt) {
    int exceptionCount = 0;

    List testdata = tdfs.getTestdata(dt);
    System.out.println("Total Samples found : " + testdata.size());
    for (Iterator<MSSQLTestData> itr = testdata.iterator(); itr.hasNext();) {
      MSSQLTestData current = itr.next();
      System.out.println("Testing with : \n" + current);

      try {

        if (dt.equals(DATATYPES.INT) || dt.equals(DATATYPES.BIGINT)
            || dt.equals(DATATYPES.SMALLINT)
            || dt.equals(DATATYPES.TINYINT)
            || dt.equals(DATATYPES.MONEY)
            || dt.equals(DATATYPES.SMALLMONEY)
            || dt.equals(DATATYPES.TIME)
            || dt.equals(DATATYPES.DATETIME)
            || dt.equals(DATATYPES.DATE)
            || dt.equals(DATATYPES.DATETIME2)
            || dt.equals(DATATYPES.DATETIMEOFFSET)
            || dt.equals(DATATYPES.REAL)
            || dt.equals(DATATYPES.FLOAT)
            || dt.equals(DATATYPES.SMALLDATETIME)
            || dt.equals(DATATYPES.NTEXT)
            || dt.equals(DATATYPES.TEXT)
            || dt.equals(DATATYPES.IMAGE)) {

          createTable(dt);
          createFile(dt, current.getData(KEY_STRINGS.HDFS_READBACK));
          runExport(getArgv(dt));
          verifyExport(dt, current.getData(KEY_STRINGS.DB_READBACK));
          addToReport(current, null);
        } else if (dt.equals(DATATYPES.DECIMAL)
            || (dt.equals(DATATYPES.NUMERIC)
                || dt.equals(DATATYPES.CHAR)
                || dt.equals(DATATYPES.VARCHAR)
                || dt.equals(DATATYPES.NCHAR)
                || dt.equals(DATATYPES.NVARCHAR)
                || dt.equals(DATATYPES.VARBINARY) || dt
                .equals(DATATYPES.BINARY))) {

          createTable(dt, current);
          createFile(dt, current.getData(KEY_STRINGS.HDFS_READBACK));
          runExport(getArgv(dt));
          verifyExport(dt, current.getData(KEY_STRINGS.DB_READBACK));
          addToReport(current, null);
        }

      } catch (AssertionError ae) {
        if (current.getData(KEY_STRINGS.NEG_POS_FLAG).equals("NEG")) {
          System.out.println("failure was expected, PASS");
          addToReport(current, null);
        } else {
          System.out
              .println("------------------------------------------------------"
                + "-----");
          System.out.println("Failure for following Test Data :\n"
              + current.toString());
          System.out
              .println("------------------------------------------------------"
                + "-----");
          System.out.println("Exception details : \n");
          System.out.println(ae.getMessage());
          System.out
              .println("------------------------------------------------------"
                + "-----");
          addToReport(current, ae);
          exceptionCount++;

        }

      } catch (Exception ae) {
        if (current.getData(KEY_STRINGS.NEG_POS_FLAG).equals("NEG")) {
          System.out.println("failure was expected, PASS");
          addToReport(current, null);
        } else {
          System.out
              .println("------------------------------------------------------"
                + "-----");
          System.out.println("Failure for following Test Data :\n"
              + current.toString());
          System.out
              .println("------------------------------------------------------"
                + "-----");
          System.out.println("Exception details : \n");
          System.out.println(ae.getMessage());
          System.out
              .println("------------------------------------------------------"
               + "-----");
          addToReport(current, ae);
          exceptionCount++;

        }

      } catch (Error e) {
        addToReport(current, e);
        exceptionCount++;
      }
    }
    if (exceptionCount > 0) {

      System.out.println("There were failures for :" + dt.toString());
      System.out.println("Failed for " + exceptionCount + "/"
          + testdata.size() + " test data samples\n");
      System.out.println("Sroll up for detailed errors");
      System.out
          .println("----------------------------------------------------------"
            + "-");
      throw new AssertionError("Failed for " + exceptionCount
          + " test data sample");
    }

  }

  /*

 */

  public String[] extractData(List data, String negPosFlag,
      KEY_STRINGS readBackType) {
    List<String> filtered = new ArrayList<String>();
    for (Iterator<MSSQLTestData> itr = data.iterator(); itr.hasNext();) {
      MSSQLTestData current = itr.next();
      if (current.getData(KEY_STRINGS.NEG_POS_FLAG).toString().equals(
          negPosFlag)) {
        filtered.add(current.getData(readBackType));
      }
    }
    String[] ret = new String[filtered.size()];
    filtered.toArray(ret);
    return ret;
  }

  public void verifyExport(DATATYPES dt, String[] data) throws SQLException {
    LOG.info("Verifying export: " + getTableName());
    // Check that we got back the correct number of records.
    Connection conn = getManager().getConnection();

    PreparedStatement statement = conn.prepareStatement("SELECT "
        + getColName() + " FROM " + getTableName(dt),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    System.out.println("data samples being compared : " + data.length);

    ResultSet rs = null;
    try {
      rs = statement.executeQuery();
      int cnt = 0;
      try {
        while (rs.next()) {
          String tmp = rs.getString(1);
          String expected = data[cnt++];
          System.out.println("Readback, expected" + tmp + " :"
              + expected);
          if (tmp == null) {
            assertNull("Must be null", expected);
          } else {
            assertEquals("Data must match", expected, tmp);
          }
        }
        System.out.println("expected samples : " + data.length
            + " Actual samples : " + cnt);
        assertEquals("Resultset must contain expected samples",
            data.length, cnt);
      } finally {

        rs.close();
      }
    } finally {
      statement.close();
    }

  }

  public void verifyNegativeExport(DATATYPES dt, String[] data)
      throws SQLException {
    LOG.info("Verifying export: " + getTableName());
    // Check that we got back the correct number of records.
    Connection conn = getManager().getConnection();

    PreparedStatement statement = conn.prepareStatement("SELECT "
        + getColName() + " FROM " + getTableName(dt),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    System.out.println("data samples being compared : " + data.length);

    ResultSet rs = null;
    try {
      rs = statement.executeQuery();
      int cnt = 0;
      try {
        while (rs.next()) {
          String tmp = rs.getString(1);
          String expected = data[cnt++];
          System.out.println("Readback, expected" + tmp + " :"
              + expected);
          if (tmp == null) {
            assertNull("Must be null", expected);
          } else {
            assertNotSame("Data must match", expected, tmp);
          }
        }

      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

  }

  public void verifyExport(DATATYPES dt, String data) throws SQLException {
    verifyExport(dt, new String[] { data });

  }

  /**
   * Run a MapReduce-based export (using the argv provided to control
   * execution).
   *
   * @return the generated jar filename
   */
  protected List<String> runExport(String[] argv) throws IOException {
    // run the tool through the normal entry-point.
    int ret;
    List<String> generatedJars = null;
    try {
      ExportTool exporter = new ExportTool();

      Sqoop sqoop = new Sqoop(exporter);

      String username = MSSQLTestUtils.getDBUserName();
      String password = MSSQLTestUtils.getDBPassWord();
      sqoop.getOptions().setUsername(username);
      sqoop.getOptions().setPassword(password);

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

  @Before
  public void setUp() {
    // start the server
    super.setUp();
    String warehouseDir = getWarehouseDir();
    Path tablePath = new Path(warehouseDir);
    try {
      String testfile = System.getProperty("test.data.dir")
          + "/" + System.getProperty("ms.datatype.test.data.file.export");
      String delim = System.getProperty("ms.datatype.test.data.file.delim", ",");
      tdfs = new MSSQLTestDataFileParser(testfile);
      tdfs.setDelim(delim);
      tdfs.parse();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      System.out.println("Error with test data file;");
      System.out
          .println("check stack trace for cause.\nTests cannont continue.");
      System.exit(0);
    }
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      fs.delete(tablePath, true);
      System.out.println("Warehouse dir deleted");
    } catch (IOException e) {
      LOG.error("Setup fail with IOException: " +
          StringUtils.stringifyException(e));
    }
    if (useHsqldbTestServer()) {
      // throw away any existing data that might be in the database.
      try {
        this.getTestServer().dropExistingSchema();
      } catch (SQLException sqlE) {
        LOG.error("Setup fail with SQLException: " +
            StringUtils.stringifyException(sqlE));
        fail(sqlE.toString());
      }
    }
  }

  protected boolean useHsqldbTestServer() {
    return false;
  }

  protected String getConnectString() {
    return System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");
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
    System.out.println("DROPing Table " + table);
    String sqlStmt = "IF OBJECT_ID('" + table
        + "') IS NOT NULL  DROP TABLE " + table;
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

    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();
    SqoopOptions opts = new SqoopOptions(conf);
    opts.setUsername(username);
    opts.setPassword(password);
    return opts;

  }

  /**
   * Create the argv to pass to Sqoop.
   *
   * @param includeHadoopFlags
   *            if true, then include -D various.settings=values
   * @param rowsPerStmt
   *            number of rows to export in a single INSERT statement.
   * @param statementsPerTx
   *            ## of statements to use in a transaction.
   * @return the argv as an array of strings.
   */
  protected String[] getArgv(DATATYPES dt) {
    ArrayList<String> args = new ArrayList<String>();

    args.add("--table");
    args.add(getTableName(dt));
    args.add("--export-dir");
    args.add(getTablePath(dt).toString());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--fields-terminated-by");
    args.add(",");
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

  public String getOutputFileName() {
    return "ManagerCompatExport.txt";
  }

  public void addToReport(MSSQLTestData td, Object result) {
    try {
      FileWriter fr = new FileWriter(getOutputFileName(), true);
      String offset = td.getData(KEY_STRINGS.OFFSET);
      String dt = td.getDatatype();
      String res = "_";
      if (result == null) {
        res = "Success";
      } else {
        try {
          res = "FAILED "
              + removeNewLines(((AssertionError) result)
                  .getMessage());

        } catch (Exception ae) {
          if (result instanceof Exception) {
            res = "FAILED "
                + removeNewLines(((Exception) result)
                    .getMessage());
          } else {
            res = "FAILED " + result.toString();
          }
        }
      }

      fr.append(offset + "\t" + "\t" + res + "\t" + dt + "\t"
          + removeNewLines(td.toString()) + "\n");
      fr.close();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }

  }

  public static String removeNewLines(String str) {
    if (str != null) {
      String[] tmp = str.split("\n");
      String result = "";
      for (String a : tmp) {
        result += " " + a;
      }
      return result;
    } else {
      return "";
    }
  }

}
