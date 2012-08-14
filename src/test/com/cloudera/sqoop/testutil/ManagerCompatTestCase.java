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

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;

import org.junit.Test;

/**
 * Class that implements common tests that should be applied to all jdbc
 * drivers that we want to interop with.
 *
 * The purpose of these tests is to ensure that if a database supports a
 * given data type, we can import this data type into Sqoop. The test is
 * not intended to check whether all data types are supported by all
 * databases, nor that the representation of a given data type has a canonical
 * representation after being imported. Some databases may not support certain
 * data types, and the format of the imported data may vary from database to
 * database. It is not Sqoop's goal to resolve inter-database differences.
 * However, if a database provides a particular type, we should verify that
 * we can import this data in some form into HDFS.
 *
 * This test battery subjects a database to a variety of import tasks. Many
 * adapter methods are provided to allow subclasses to modify the exact type
 * names injected, expected output values, etc., to account for inter-database
 * discrepencies.
 *
 * Each subclass of this class should test a single ConnManager implementation.
 * Subclasses must implement all abstract methods of this class. They may
 * also wish to override several members of the class hierarchy above this.
 * In particular:
 *
 * String getConnectString() -- Return the connect string to use to get the db.
 * void dropTableIfExists(tableName) -- how to drop a table that may not exist.
 * void createTableWithColTypes() -- how to create a table with a set of cols.
 * Configuration getConf() -- specifies config properties specific to a test.
 * SqoopOptions getSqoopOptions(conf) -- Instantiates the SqoopOptions to use.
 * List&lt;String&gt; getExtraArgs() -- specifies extra argv elements.
 */
public abstract class ManagerCompatTestCase extends ImportJobTestCase {

  private Log log;

  public ManagerCompatTestCase() {
    this.log = LogFactory.getLog(ManagerCompatTestCase.class.getName());
  }

  /**
   * @return the Log object to use for reporting during this test
   */
  protected abstract Log getLogger();

  /**
   * @return a "friendly" name for the database. e.g "mysql" or "oracle".
   */
  protected abstract String getDbFriendlyName();

  /** Set to true during tearDown() if a test is skipped. */
  protected boolean skipped;

  @Override
  protected String getTablePrefix() {
    return "MGR_" + getDbFriendlyName().toUpperCase() + "_";
  }

  @Override
  protected boolean useHsqldbTestServer() {
    // Compat tests, by default, do not use hsqldb.
    return false;
  }

  @Override
  public void setUp() {
    log = getLogger();
    skipped = false;
    super.setUp();
  }

  @Override
  public void tearDown() {
    try {
      // Clean up the database on our way out.
      dropTableIfExists(getTableName());
    } catch (SQLException e) {
      log.warn("Error trying to drop table '" + getTableName()
          + "' on tearDown: " + e);
    }
    super.tearDown();
  }

  //////// These methods indicate whether certain datatypes are supported
  //////// by the underlying database.

  /** @return true if the database under test has a BOOLEAN type */
  protected boolean supportsBoolean() {
    return true;
  }

  /** @return true if the database under test has a BIGINT type */
  protected boolean supportsBigInt() {
    return true;
  }

  /** @return true if the database under test has a TINYINT type */
  protected boolean supportsTinyInt() {
    return true;
  }

  /** @return true if the database under test has a LONGVARCHAR type */
  protected boolean supportsLongVarChar() {
    return true;
  }

  /** @return true if the database under test has a VARBINARY type */
  protected boolean supportsVarBinary() {
    return true;
  }

  /** @return true if the database under test has a TIME type */
  protected boolean supportsTime() {
    return true;
  }

  /** @return true if the database under test supports CLOB types */
  protected boolean supportsClob() {
    return true;
  }

  /** @return true if the database under test supports BLOB types */
  protected boolean supportsBlob() {
    return true;
  }

  //////// These methods indicate how to define various datatypes.

  /**
   * Define a NUMERIC type that can handle 30 digits total, and 5
   * digits to the right of the decimal point.
   */
  protected String getNumericType() {
    return "NUMERIC(" + getNumericScale() + ", "
        + getNumericDecPartDigits() + ")";
  }

  protected String getDecimalType() {
    return "DECIMAL(" + getDecimalScale() + ", "
        + getDecimalDecPartDigits() + ")";
  }

  /**
   * Return the number of digits to use in the integral part of a
   * NUMERIC type.
   */
  protected int getNumericScale() {
    return 30;
  }

  /**
   * Return the number of digits to use in the decimal part of a
   * NUMERIC type.
   */
  protected int getNumericDecPartDigits() {
    return 5;
  }

  /**
   * Return the number of digits to use in the integral part of a
   * DECIMAL type.
   */
  protected int getDecimalScale() {
    return 30;
  }

  /**
   * Return the number of digits to use in the decimal part of a
   * DECIMAL type.
   */
  protected int getDecimalDecPartDigits() {
    return 5;
  }

  /**
   * Define a DOUBLE column.
   */
  protected String getDoubleType() {
    return "DOUBLE";
  }

  /**
   * Define a LONGVARCHAR type that can handle at least 24 characters.
   */
  protected String getLongVarCharType() {
    return "LONGVARCHAR";
  }

  /**
   * Define a TIMESTAMP type that can handle null values.
   */
  protected String getTimestampType() {
    return "TIMESTAMP";
  }

  /**
   * Define a CLOB column that can contain up to 16 MB of data.
   */
  protected String getClobType() {
    return "CLOB";
  }

  /**
   * Define a BLOB column that can contain up to 16 MB of data.
   */
  protected String getBlobType() {
    return "BLOB";
  }

  /**
   * Define a VARBINARY column that can contain up to 12 bytes of data.
   */
  protected String getVarBinaryType() {
    return "VARBINARY(12)";
  }

  /**
   * Define a TINYINT column that can contain 8-bit signed integers.
   */
  protected String getTinyIntType() {
    return "TINYINT";
  }

  //////// These methods indicate how databases respond to various datatypes.
  //////// Since our comparisons are all string-based, these return strings.

  /** @return How we insert the value TRUE represented as an int. */
  protected String getTrueBoolNumericSqlInput() {
    return "1";
  }

  /** @return How we insert the value FALSE represented as an int. */
  protected String getFalseBoolNumericSqlInput() {
    return "0";
  }

  /** @return How we insert the value TRUE represented as a boolean literal. */
  protected String getTrueBoolLiteralSqlInput() {
    return "true";
  }

  /** @return How we insert the value FALSE represented as a boolean literal. */
  protected String getFalseBoolLiteralSqlInput() {
    return "false";
  }

  /** @return How a BOOLEAN column with value TRUE is represented in a seq-file
   * import. */
  protected String getTrueBoolSeqOutput() {
    return "true";
  }

  /** @return How a BOOLEAN column with value FALSE is represented in a seq-file
   * import. */
  protected String getFalseBoolSeqOutput() {
    return "false";
  }

  protected String padString(int width, String str) {
    int extra = width - str.length();
    for (int i = 0; i < extra; i++) {
      str = str + " ";
    }

    return str;
  }

  /**
   * helper method: return a floating-point string in the same way
   * it was entered, but integers get a trailing '.0' attached.
   */
  protected String withDecimalZero(String floatingPointStr) {
    if (floatingPointStr.indexOf(".") == -1) {
      return floatingPointStr + ".0";
    } else {
      return floatingPointStr;
    }
  }

  /**
   * @return how a given real value is represented in an imported sequence
   * file
   */
  protected String getRealSeqOutput(String realAsInserted) {
    return withDecimalZero(realAsInserted);
  }

  /**
   * @return how a given float value is represented in an imported sequence
   * file
   */
  protected String getFloatSeqOutput(String floatAsInserted) {
    return withDecimalZero(floatAsInserted);
  }

  /**
   * @return how a given double value is represented in an imported sequence
   * file
   */
  protected String getDoubleSeqOutput(String doubleAsInserted) {
    return withDecimalZero(doubleAsInserted);
  }

  /**
   * Some databases require that we insert dates using a special format.
   * This takes the canonical string used to insert a DATE into a table,
   * and specializes it to the SQL dialect used by the database under
   * test.
   */
  protected String getDateInsertStr(String insertStr) {
    return insertStr;
  }

  /**
   * Some databases require that we insert times using a special format.
   * This takes the canonical string used to insert a TIME into a table,
   * and specializes it to the SQL dialect used by the database under
   * test.
   */
  protected String getTimeInsertStr(String insertStr) {
    return insertStr;
  }

  /**
   * Some databases require that we insert timestamps using a special format.
   * This takes the canonical string used to insert a TIMESTAMP into a table,
   * and specializes it to the SQL dialect used by the database under
   * test.
   */
  protected String getTimestampInsertStr(String insertStr) {
    return insertStr;
  }

  protected String getDateSeqOutput(String dateAsInserted) {
    return dateAsInserted;
  }

  /**
   * Convert an input timestamp to the string representation of the timestamp
   * returned by a sequencefile-based import.
   *
   * @param tsAsInserted the input timestamp
   * @return the string version of this as returned by the database is
   * represented.
   */
  protected String getTimestampSeqOutput(String tsAsInserted) {
    if ("null".equals(tsAsInserted)) {
      return tsAsInserted;
    }

    int dotPos = tsAsInserted.indexOf(".");
    if (-1 == dotPos) {
      // No dot in the original string; expand to add a single item after the
      // dot.
      return tsAsInserted + ".0";
    } else {
      // all other strings return as-is.
      return tsAsInserted;
    }
  }

  protected String getNumericSeqOutput(String numAsInserted) {
    return numAsInserted;
  }

  protected String getDecimalSeqOutput(String numAsInserted) {
    return numAsInserted;
  }

  /**
   * @return how a CHAR(fieldWidth) field is represented in an imported
   * sequence file
   */
  protected String getFixedCharSeqOut(int fieldWidth, String asInserted) {
    return asInserted;
  }

  /**
   * Encode a string to be inserted in a BLOB field.
   * @param blobData the raw text (Without quote marks) to insert for a BLOB.
   * @return 'blobData' in a String form ready for insertion
   */
  protected String getBlobInsertStr(String blobData) {
    return "'" + blobData + "'";
  }

  /**
   * @return A byte array declaring how an inserted BLOB will be returned to
   * us via the database.
   */
  protected byte [] getBlobDbOutput(String asInserted) {
    // The database will give us back a byte array; we need to create
    // an identical byte array.
    try {
      return asInserted.getBytes("UTF-8");
    } catch (UnsupportedEncodingException uee) {
      fail("Could not get utf8 bytes"); // Java should always support UTF-8.
      return null;
    }
  }

  /**
   * @return A String declaring how an inserted BLOB will be returned to
   * us via the sequencefile.
   */
  protected String getBlobSeqOutput(String asInserted) {
    return new BytesWritable(getBlobDbOutput(asInserted)).toString();
  }

  /**
   * @return A String declaring how an inserted VARBINARY will be
   * returned to us via the sequencefile.
   */
  protected String getVarBinarySeqOutput(String asInserted) {
    return new BytesWritable(getBlobDbOutput(asInserted)).toString();
  }

  /**
   * Given a string of characters which represent hex values
   * (e.g., 'ABF00F1238'), return the string as a set of separated
   * octets, in lower case (e.g., 'ab f0 0f 12 38').
   *
   * @param str the input string of hex digits
   * @return the input string as space-separated lower-case octets.
   */
  protected String toLowerHexString(String str) {
    // The inserted text is a hex string of the form 'ABABABAB'.
    // We return it in the form 'ab ab ab ab'.
    StringBuilder sb = new StringBuilder();
    boolean isOdd = false;
    boolean first = true;
    for (char c : str.toCharArray()) {
      if (!isOdd && !first) {
        sb.append(' ');
      }

      sb.append(Character.toLowerCase(c));
      isOdd = !isOdd;
      first = false;
    }

    return sb.toString();
  }

  //////// The actual tests occur below here. ////////

  /**
   * Do a full verification test on the singleton value of a given type.
   * @param colType  The SQL type to instantiate the column.
   * @param insertVal The SQL text to insert a value into the database.
   * @param seqFileVal The string representation of the value as extracted
   *        through the DBInputFormat, serialized, and injected into a
   *        SequenceFile and put through toString(). This may be slightly
   *        different than what ResultSet.getString() returns, which is used
   *        by returnVal.
   */
  protected void verifyType(String colType, String insertVal,
      String seqFileVal) {
    verifyType(colType, insertVal, seqFileVal, false);
  }

  protected void verifyType(String colType, String insertVal, String seqFileVal,
      boolean useIntPrimaryKey) {

    String readbackPrepend = "";

    if (useIntPrimaryKey) {
      String [] types = { "INTEGER", colType };
      String [] vals = { "0", insertVal };
      createTableWithColTypes(types, vals);
      readbackPrepend = "0,"; // verifyImport will verify the entire row.
    } else {
      createTableForColType(colType, insertVal);
    }

    verifyImport(readbackPrepend + seqFileVal, null);
  }

  static final String STRING_VAL_IN = "'this is a short string'";
  static final String STRING_VAL_OUT = "this is a short string";

  @Test
  public void testStringCol1() {
    verifyType("VARCHAR(32)", STRING_VAL_IN, STRING_VAL_OUT);
  }

  @Test
  public void testStringCol2() {
    verifyType("CHAR(32)", STRING_VAL_IN,
        getFixedCharSeqOut(32, STRING_VAL_OUT));
  }

  @Test
  public void testEmptyStringCol() {
    verifyType("VARCHAR(32)", "''", "");
  }

  @Test
  public void testNullStringCol() {
    verifyType("VARCHAR(32)", "NULL", null);
  }

  @Test
  public void testInt() {
    verifyType("INTEGER", "42", "42");
  }

  @Test
  public void testNullInt() {
    verifyType("INTEGER", "NULL", null);
  }

  @Test
  public void testBoolean() {
    if (!supportsBoolean()) {
      log.info("Skipping boolean test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("BOOLEAN", getTrueBoolNumericSqlInput(),
        getTrueBoolSeqOutput());
  }

  @Test
  public void testBoolean2() {
    if (!supportsBoolean()) {
      log.info("Skipping boolean test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("BOOLEAN", getFalseBoolNumericSqlInput(),
        getFalseBoolSeqOutput());
  }

  @Test
  public void testBoolean3() {
    if (!supportsBoolean()) {
      log.info("Skipping boolean test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("BOOLEAN", getFalseBoolLiteralSqlInput(),
        getFalseBoolSeqOutput());
  }

  @Test
  public void testTinyInt1() {
    if (!supportsTinyInt()) {
      log.info("Skipping tinyint test (unsupported)");
      skipped = true;
      return;
    }
    verifyType(getTinyIntType(), "0", "0");
  }

  @Test
  public void testTinyInt2() {
    if (!supportsTinyInt()) {
      log.info("Skipping tinyint test (unsupported)");
      skipped = true;
      return;
    }
    verifyType(getTinyIntType(), "42", "42");
  }

  @Test
  public void testSmallInt1() {
    verifyType("SMALLINT", "-1024", "-1024");
  }

  @Test
  public void testSmallInt2() {
    verifyType("SMALLINT", "2048", "2048");
  }

  @Test
  public void testBigInt1() {
    if (!supportsBigInt()) {
      log.info("Skipping bigint test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("BIGINT", "10000000000", "10000000000");
  }


  @Test
  public void testReal1() {
    verifyType("REAL", "256", getRealSeqOutput("256"));
  }

  @Test
  public void testReal2() {
    verifyType("REAL", "256.45", getRealSeqOutput("256.45"));
  }

  @Test
  public void testFloat1() {
    verifyType("FLOAT", "256", getFloatSeqOutput("256"));
  }

  @Test
  public void testFloat2() {
    verifyType("FLOAT", "256.5", getFloatSeqOutput("256.5"));
  }

  @Test
  public void testDouble1() {
    verifyType(getDoubleType(), "-256", getDoubleSeqOutput("-256"));
  }

  @Test
  public void testDouble2() {
    verifyType(getDoubleType(), "256.45", getDoubleSeqOutput("256.45"));
  }

  @Test
  public void testDate1() {
    verifyType("DATE", getDateInsertStr("'2009-01-12'"),
        getDateSeqOutput("2009-01-12"));
  }

  @Test
  public void testDate2() {
    verifyType("DATE", getDateInsertStr("'2009-04-24'"),
        getDateSeqOutput("2009-04-24"));
  }

  @Test
  public void testTime1() {
    if (!supportsTime()) {
      log.info("Skipping time test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("TIME", getTimeInsertStr("'12:24:00'"), "12:24:00");
  }

  @Test
  public void testTime2() {
    if (!supportsTime()) {
      log.info("Skipping time test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("TIME", getTimeInsertStr("'06:24:00'"), "06:24:00");
  }

  @Test
  public void testTime3() {
    if (!supportsTime()) {
      log.info("Skipping time test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("TIME", getTimeInsertStr("'6:24:00'"), "06:24:00");
  }

  @Test
  public void testTime4() {
    if (!supportsTime()) {
      log.info("Skipping time test (unsupported)");
      skipped = true;
      return;
    }
    verifyType("TIME", getTimeInsertStr("'18:24:00'"), "18:24:00");
  }

  @Test
  public void testTimestamp1() {
    verifyType(getTimestampType(),
        getTimestampInsertStr("'2009-04-24 18:24:00'"),
        getTimestampSeqOutput("2009-04-24 18:24:00"));
  }

  @Test
  public void testTimestamp2() {
    try {
      log.debug("Beginning testTimestamp2");
      verifyType(getTimestampType(),
          getTimestampInsertStr("'2009-04-24 18:24:00.0002'"),
          getTimestampSeqOutput("2009-04-24 18:24:00.0002"));
    } finally {
      log.debug("End testTimestamp2");
    }
  }

  @Test
  public void testTimestamp3() {
    try {
      log.debug("Beginning testTimestamp3");
      verifyType(getTimestampType(), "null", null);
    } finally {
      log.debug("End testTimestamp3");
    }
  }

  @Test
  public void testNumeric1() {
    verifyType(getNumericType(), "1",
        getNumericSeqOutput("1"));
  }

  @Test
  public void testNumeric2() {
    verifyType(getNumericType(), "-10",
        getNumericSeqOutput("-10"));
  }

  @Test
  public void testNumeric3() {
    verifyType(getNumericType(), "3.14159",
        getNumericSeqOutput("3.14159"));
  }

  @Test
  public void testNumeric4() {
    verifyType(getNumericType(),
        "3000000000000000000.14159",
        getNumericSeqOutput("3000000000000000000.14159"));
  }

  @Test
  public void testNumeric5() {
    verifyType(getNumericType(),
        "99999999999999999999.14159",
        getNumericSeqOutput("99999999999999999999.14159"));

  }

  @Test
  public void testNumeric6() {
    verifyType(getNumericType(),
        "-99999999999999999999.14159",
        getNumericSeqOutput("-99999999999999999999.14159"));
  }

  @Test
  public void testDecimal1() {
    verifyType(getDecimalType(), "1",
        getDecimalSeqOutput("1"));
  }

  @Test
  public void testDecimal2() {
    verifyType(getDecimalType(), "-10",
        getDecimalSeqOutput("-10"));
  }

  @Test
  public void testDecimal3() {
    verifyType(getDecimalType(), "3.14159",
        getDecimalSeqOutput("3.14159"));
  }

  @Test
  public void testDecimal4() {
    verifyType(getDecimalType(),
        "3000000000000000000.14159",
        getDecimalSeqOutput("3000000000000000000.14159"));
  }

  @Test
  public void testDecimal5() {
    verifyType(getDecimalType(),
        "99999999999999999999.14159",
        getDecimalSeqOutput("99999999999999999999.14159"));
  }

  @Test
  public void testDecimal6() {
    verifyType(getDecimalType(),
        "-99999999999999999999.14159",
        getDecimalSeqOutput("-99999999999999999999.14159"));
  }

  @Test
  public void testLongVarChar() {
    if (!supportsLongVarChar()) {
      log.info("Skipping long varchar test (unsupported)");
      skipped = true;
      return;
    }
    verifyType(getLongVarCharType(),
        "'this is a long varchar'",
        "this is a long varchar");
  }


  protected void verifyClob(String insertVal, String returnVal,
      String seqFileVal) {
    String [] types = { "INTEGER NOT NULL", getClobType() };
    String [] vals = { "1", insertVal };
    String [] checkCol = { "DATA_COL0", "DATA_COL1" };

    createTableWithColTypes(types, vals);
    verifyImport("1," + seqFileVal, checkCol);
  }

  protected void verifyBlob(String insertVal, byte [] returnVal,
      String seqFileVal) {
    String [] types = { "INTEGER NOT NULL", getBlobType() };
    String [] vals = { "1", insertVal };
    String [] checkCols = { "DATA_COL0", "DATA_COL1" };

    createTableWithColTypes(types, vals);

    // Verify readback of the data.
    ResultSet results = null;
    try {
      results = getManager().readTable(getTableName(), getColNames());
      assertNotNull("Null results from readTable()!", results);
      assertTrue("Expected at least one row returned", results.next());
      Blob blob = results.getBlob(2);
      byte [] databaseBytes = blob.getBytes(1, (int) blob.length());
      log.info("Verifying readback of bytes from " + getTableName());

      assertEquals("byte arrays differ in size", returnVal.length,
          databaseBytes.length);
      for (int i = 0; i < returnVal.length; i++) {
        assertEquals("bytes differ at position " + i + ". Expected "
            + returnVal[i] + "; got " + databaseBytes[i],
            returnVal[i],
            databaseBytes[i]);
      }

      assertFalse("Expected at most one row returned", results.next());
    } catch (SQLException sqlE) {
      fail("Got SQLException: " + sqlE.toString());
    } finally {
      if (null != results) {
        try {
          results.close();
        } catch (SQLException sqlE) {
          fail("Got SQLException in resultset.close(): " + sqlE.toString());
        }
      }

      // Free internal resources after the readTable.
      getManager().release();
    }

    // Now verify that we can use the Sqoop import mechanism on this data.
    verifyImport("1," + seqFileVal, checkCols);
  }


  @Test
  public void testClob1() {
    if (!supportsClob()) {
      log.info("Skipping CLOB test; database does not support CLOB");
      return;
    }

    verifyClob("'This is short CLOB data'",
        "This is short CLOB data",
        "This is short CLOB data");
  }

  @Test
  public void testBlob1() {
    if (!supportsBlob()) {
      log.info("Skipping BLOB test; database does not support BLOB");
      return;
    }

    verifyBlob(getBlobInsertStr("This is short BLOB data"),
        getBlobDbOutput("This is short BLOB data"),
        getBlobSeqOutput("This is short BLOB data"));
  }

  @Test
  public void testVarBinary() {
    if (!supportsVarBinary()) {
      log.info("Skipping VARBINARY test; database does not support VARBINARY");
      return;
    }

    verifyType(getVarBinaryType(), "'F00FABCD'",
        getVarBinarySeqOutput("F00FABCD"), true);
  }
}

