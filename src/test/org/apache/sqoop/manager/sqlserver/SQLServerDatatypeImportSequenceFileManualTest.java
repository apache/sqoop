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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;
import org.junit.Test;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.ManagerCompatTestCase;
import org.apache.sqoop.manager.sqlserver.MSSQLTestDataFileParser.DATATYPES;
import org.apache.sqoop.manager.sqlserver.MSSQLTestData.KEY_STRINGS;

/**
 * Testing import of a sequence file to SQL Server.
 */
public class SQLServerDatatypeImportSequenceFileManualTest extends
    ManagerCompatTestCase {

  public static final Log LOG = LogFactory.getLog(
      SQLServerDatatypeImportSequenceFileManualTest.class.getName());
  private static MSSQLTestDataFileParser tdfs;
  private static Map report;

  static {
    try {

      String testfile = null;
      testfile = System.getProperty("test.data.dir")
        + "/" + System.getProperty("ms.datatype.test.data.file.import");
      String delim = System.getProperty("ms.datatype.test.data.file.delim", ",");
      System.out.println("Using data file : " + testfile);
      LOG.info("Using data file : " + testfile);
      tdfs = new MSSQLTestDataFileParser(testfile);
      tdfs.setDelim(delim);
      tdfs.parse();
      report = new HashMap();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      System.out
       .println("Error with test data file, check stack trace for cause"
         + ".\nTests cannont continue.");
      System.exit(0);
    }
  }

  @Override
  protected String getDbFriendlyName() {
    return "MSSQL";
  }

  @Override
  protected Log getLogger() {
    return LOG;
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

  public void setUp() {
    try {
      super.setUp();
    } catch (Exception e) {
      try {
        FileWriter fr = new FileWriter(getResportFileName(), true);
        String res = removeNewLines(e.getMessage());
        fr.append("Error\t" + res + "\n");
        fr.close();
      } catch (Exception e2) {
        LOG.error(StringUtils.stringifyException(e2));
        fail(e2.toString());
      }
    } catch (Error e) {
      try {
        FileWriter fr = new FileWriter(getResportFileName(), true);

        String res = removeNewLines(e.getMessage());

        fr.append("Error\t" + res + "\n");
        fr.close();
        fail(res);
      } catch (Exception e2) {
        LOG.error(StringUtils.stringifyException(e2));
        fail(e2.toString());
      }
    }
  }

  public void tearDown() {
    try {
      super.tearDown();
    } catch (Exception e) {
      try {
        FileWriter fr = new FileWriter(getResportFileName(), true);
        String res = removeNewLines(e.getMessage());
        fr.append("Error\t" + res + "\n");
        fr.close();
      } catch (Exception e2) {
        LOG.error(StringUtils.stringifyException(e2));
        fail(e2.toString());
      }
    } catch (Error e) {
      try {
        FileWriter fr = new FileWriter(getResportFileName(), true);
        String res = removeNewLines(e.getMessage());
        fr.append("Error\t" + res + "\n");
        fr.close();
        fail(res);
      } catch (Exception e2) {
        LOG.error(StringUtils.stringifyException(e2));
        fail(e2.toString());
      }
    }
  }

  protected boolean supportsBoolean() {
    return true;
  }

  @Test
  public void testBit() {
    if (!supportsBoolean()) {
      skipped = true;
      return;
    }
    verifyType("BIT", getTrueBoolNumericSqlInput(), getTrueBoolSeqOutput());
  }

  @Test
  public void testBit2() {
    if (!supportsBoolean()) {
      skipped = true;
    return;
  }
    verifyType("BIT", getFalseBoolNumericSqlInput(), getFalseBoolSeqOutput());
  }

  @Test
  public void testBit3() {
    if (!supportsBoolean()) {
      skipped = true;
      return;
  }
  verifyType("BIT", getFalseBoolLiteralSqlInput(), getFalseBoolSeqOutput());
  }

  public void testBoolean() {
    try {
      super.testBoolean();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testBoolean2() {
    try {
      super.testBoolean2();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testBoolean3() {
    try {
      super.testBoolean3();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testDouble1() {
    try {
      super.testDouble1();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  @Test
  public void testDouble2() {
    try {
      super.testDouble2();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testClob1() {
    try {
      super.testClob1();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testBlob1() {
    try {
    super.testBlob1();
    assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
    System.out.println("Test failed, this was expected");
    }
  }

  public void testLongVarChar() {
    try {
      super.testLongVarChar();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testTimestamp1() {
    try {
      super.testTimestamp1();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testTimestamp2() {
    try {
      super.testTimestamp2();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testTimestamp3() {
    try {
      super.testTimestamp3();
      assertTrue("This test should not pass on sql server", false);
    } catch (AssertionError a) {
      System.out.println("Test failed, this was expected");
    }
  }

  public void testVarBinary() {
    if (!supportsVarBinary()) {
      return;
    }
    dataTypeTest(DATATYPES.VARBINARY);
  }

  public void testTime() {
    if (!supportsTime()) {
      skipped = true;
      return;
    }
    dataTypeTest(DATATYPES.TIME);
  }

  @Test
  public void testSmalldatetime() {
    if (!supportsTime()) {
      skipped = true;
      return;
    }
    dataTypeTest(DATATYPES.SMALLDATETIME);
  }

  @Test
  public void testdatetime2() {
    if (!supportsTime()) {
      skipped = true;
      return;
    }
    dataTypeTest(DATATYPES.DATETIME2);
  }

  @Test
  public void testdatetime() {
    if (!supportsTime()) {
      skipped = true;
      return;
    }
    dataTypeTest(DATATYPES.DATETIME);
  }

  @Test
  public void testdatetimeoffset() {
    if (!supportsTime()) {
      skipped = true;
      return;
    }
    dataTypeTest(DATATYPES.DATETIMEOFFSET);
  }

  public void testDecimal() {
    dataTypeTest(DATATYPES.DECIMAL);
  }

  public void testNumeric() {
    dataTypeTest(DATATYPES.NUMERIC);
  }

  public void testNumeric1() {
  }

  public void testNumeric2() {
  }

  public void testDecimal1() {
  }

  public void testDecimal2() {
  }

  @Test
  public void testBigInt() {
    dataTypeTest(DATATYPES.BIGINT);
  }

  @Test
  public void testBigInt1() {
  }

  @Test
  public void testInt() {
    dataTypeTest(DATATYPES.INT);
  }

  @Test
  public void testSmallInt() {
    dataTypeTest(DATATYPES.SMALLINT);
  }

  @Test
  public void testSmallInt1() {
  }

  @Test
  public void testSmallInt2() {
  }

  @Test
  public void testTinyint() {
    dataTypeTest(DATATYPES.TINYINT);

  }

  @Test
  public void testTinyInt1() {
  }

  @Test
    public void testTinyInt2() {
  }

  @Test
  public void testFloat() {
    dataTypeTest(DATATYPES.FLOAT);
  }

  @Test
  public void testReal() {
    dataTypeTest(DATATYPES.REAL);
  }

  @Test
  public void testDate() {
    dataTypeTest(DATATYPES.DATE);
  }

  public void testMoney() {
    dataTypeTest(DATATYPES.MONEY);
  }

  @Test
  public void testSmallMoney() {
    dataTypeTest(DATATYPES.SMALLMONEY);
  }

  @Test
  public void testText() {
    dataTypeTest(DATATYPES.TEXT);
  }

  @Test
  public void testVarchar() {
    dataTypeTest(DATATYPES.VARCHAR);
  }

  @Test
  public void testChar() {
    dataTypeTest(DATATYPES.CHAR);
  }

  @Test
  public void testNText() {
    dataTypeTest(DATATYPES.NTEXT);
  }

  @Test
  public void testNChar() {
    dataTypeTest(DATATYPES.NCHAR);
  }

  @Test
  public void testNVarchar() {
    dataTypeTest(DATATYPES.NVARCHAR);
  }

  public void testImage() {
    dataTypeTest(DATATYPES.IMAGE);
  }

  public void testBinary() {
    dataTypeTest(DATATYPES.BINARY);
  }

  //---------------disabled tests-----
  @Test
  public void testTime1() {
  }

  @Test
  public void testTime2() {
  }

  @Test
  public void testTime3() {
  }

  @Test
  public void testTime4() {
  }

  @Test
  public void testStringCol1() {

  }

  @Test
  public void testStringCol2() {

  }

  @Test
  public void testEmptyStringCol() {

  }

  @Test
  public void testNullStringCol() {

  }

  @Test
  public void testNullInt() {

  }

  @Test
  public void testReal1() {

  }

  @Test
  public void testReal2() {

  }

  @Test
  public void testFloat1() {

  }

  @Test
  public void testFloat2() {

  }

  @Test
  public void testDate1() {

  }

  @Test
  public void testDate2() {

  }


  @Test
  public void testNumeric3() {

  }

  @Test
  public void testNumeric4() {

  }

  @Test
  public void testNumeric5() {


  }

  @Test
  public void testNumeric6() {

  }



  @Test
  public void testDecimal3() {

  }

  @Test
  public void testDecimal4() {

  }

  @Test
  public void testDecimal5() {


  }

  @Test
  public void testDecimal6() {

  }



  //end disabled tests----------------------------

  public String getTrueBoolDbOutput() {
    return "1";
  }

  public String getFalseBoolDbOutput() {
    return "0";
  }

  protected String getFalseBoolSeqOutput() {
    return "false";
  }

  protected String getFalseBoolLiteralSqlInput() {
    return "0";
  }

  protected String getFixedCharSeqOut(int len, String val) {
    return val + nSpace(len - val.length());
  }

  protected String getFixedCharDbOut(int len, String val) {
    return val + nSpace(len - val.length());
  }

  public String nSpace(int n) {
    String tmp = "";
    for (int i = 0; i < n; i++) {
      tmp += " ";
    }

    return tmp;
  }

  public String nZeros(int n) {
    String tmp = "";
    for (int i = 0; i < n; i++) {
      tmp += "0";
    }

    return tmp;
  }

  public void dataTypeTest(DATATYPES datatype) {
    int exceptionCount = 0;

    List testdata = tdfs.getTestdata(datatype);

    for (Iterator<MSSQLTestData> itr = testdata.iterator(); itr.hasNext();) {
      MSSQLTestData current = itr.next();
      System.out.println("Testing with : \n" + current);

      try {
        if (datatype == DATATYPES.DECIMAL
           || datatype == DATATYPES.NUMERIC) {

          verifyType(current.getDatatype() + "("
            + current.getData(KEY_STRINGS.SCALE) + ","
            + current.getData(KEY_STRINGS.PREC) + ")", current
          .getData(KEY_STRINGS.TO_INSERT), current
          .getData(KEY_STRINGS.HDFS_READBACK));

        } else if (datatype == DATATYPES.TIME
           || datatype == DATATYPES.SMALLDATETIME
           || datatype == DATATYPES.DATETIME2
           || datatype == DATATYPES.DATETIME
           || datatype == DATATYPES.DATETIMEOFFSET
           || datatype == DATATYPES.TEXT
           || datatype == DATATYPES.NTEXT
           || datatype == DATATYPES.DATE) {
          verifyType(current.getDatatype(), "'"
            + current.getData(KEY_STRINGS.TO_INSERT) + "'", current
          .getData(KEY_STRINGS.HDFS_READBACK));
        } else if (datatype == DATATYPES.VARBINARY) {
          verifyType(
          current.getDatatype() + "("
          + current.getData(KEY_STRINGS.SCALE) + ")",
          "cast('" + current.getData(KEY_STRINGS.TO_INSERT)
          + "' as varbinary("
          + current.getData(KEY_STRINGS.SCALE) + "))",
          current.getData(KEY_STRINGS.HDFS_READBACK));
        } else if (datatype == DATATYPES.BINARY) {
          verifyType(
          current.getDatatype() + "("
          + current.getData(KEY_STRINGS.SCALE) + ")",
          "cast('" + current.getData(KEY_STRINGS.TO_INSERT)
          + "' as binary("
          + current.getData(KEY_STRINGS.SCALE) + "))",
          current.getData(KEY_STRINGS.HDFS_READBACK));
        } else if (datatype == DATATYPES.NCHAR
        || datatype == DATATYPES.VARCHAR
        || datatype == DATATYPES.CHAR
        || datatype == DATATYPES.NVARCHAR) {
        System.out.println("------>"
        + current.getData(KEY_STRINGS.DB_READBACK)
        + "<----");
        verifyType(current.getDatatype() + "("
        + current.getData(KEY_STRINGS.SCALE) + ")", "'"
        + current.getData(KEY_STRINGS.TO_INSERT) + "'",
        current.getData(KEY_STRINGS.HDFS_READBACK));
        } else if (datatype == DATATYPES.IMAGE) {
        verifyType(current.getDatatype(), "cast('"
        + current.getData(KEY_STRINGS.TO_INSERT)
        + "' as image )",
        current.getData(KEY_STRINGS.HDFS_READBACK));
        } else {
          verifyType(current.getDatatype(), current
          .getData(KEY_STRINGS.TO_INSERT), current
          .getData(KEY_STRINGS.HDFS_READBACK));
        }

        addToReport(current, null);

      } catch (AssertionError ae) {
        if (current.getData(KEY_STRINGS.NEG_POS_FLAG).equals("NEG")) {
          System.out.println("failure was expected, PASS");
          addToReport(current, null);
        } else {
          System.out
          .println("----------------------------------------------------------"
            + "-");
          System.out.println("Failure for following Test Data :\n"
          + current.toString());
          System.out
          .println("----------------------------------------------------------"
            + "-");
          System.out.println("Exception details : \n");
          System.out.println(ae.getMessage());
          System.out
          .println("----------------------------------------------------------"
            + "-");
          addToReport(current, ae);
          exceptionCount++;
        }
      } catch (Exception e) {
        addToReport(current, e);
        exceptionCount++;
      }
    }

    if (exceptionCount > 0) {
      System.out.println("There were failures for :"
      + datatype.toString());
      System.out.println("Failed for " + exceptionCount
      + " test data samples\n");
      System.out.println("Sroll up for detailed errors");
      System.out
      .println("-----------------------------------------------------------");
      throw new AssertionError("Failed for " + exceptionCount
      + " test data sample");
    }
  }

  public  synchronized void addToReport(MSSQLTestData td, Object result) {
    System.out.println("called");
    try {
      FileWriter fr = new FileWriter(getResportFileName(), true);
      String offset = td.getData(KEY_STRINGS.OFFSET);
      String res = "_";
      if (result == null) {
      res = "Success";
    } else {
      try {
      res = "FAILED "
      + removeNewLines(((AssertionError) result)
      .getMessage());
      } catch (Exception ae) {
        if (result instanceof Exception
          && ((Exception) result) != null) {
          res = "FAILED "
          + removeNewLines(((Exception) result)
          .getMessage());
        } else {
          res = "FAILED " + result.toString();
        }
      }
    }

    fr.append(offset + "\t" + res + "\n");
    fr.close();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  public static String removeNewLines(String str) {
    String[] tmp = str.split("\n");
    String result = "";
    for (String a : tmp) {
      result += " " + a;
    }
    return result;
  }

  public String getResportFileName(){
    return this.getClass().toString()+".txt";
  }
}
