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

package com.cloudera.sqoop.manager;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Formatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.ManagerCompatTestCase;

import junit.framework.AssertionFailedError;

/**
 * Test the basic Oracle connection manager with the various column types.
 */
public class OracleCompatTest extends ManagerCompatTestCase {

  public static final Log LOG = LogFactory.getLog(
      OracleCompatTest.class.getName());

  @Override
  protected Log getLogger() {
    return LOG;
  }

  @Override
  protected String getDbFriendlyName() {
    return "Oracle";
  }

  @Override
  protected String getConnectString() {
    return OracleUtils.CONNECT_STRING;
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    OracleUtils.setOracleAuth(opts);
    return opts;

  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    OracleUtils.dropTable(table, getManager());
  }

  @Override
  public void tearDown() {
    super.tearDown();

    // If we actually ran the test, we'll need to 'cool off' afterwards.
    if (!skipped) {
      // Oracle XE will block connections if you create new ones too quickly.
      // See http://forums.oracle.com/forums/thread.jspa?messageID=1145120
      LOG.info("Sleeping to wait for Oracle connection cache clear...");
      try {
        Thread.sleep(250);
      } catch (InterruptedException ie) {
        // This delay may run a bit short.. no problem.
      }
    }
  }

  @Override
  protected String getDoubleType() {
    return "DOUBLE PRECISION";
  }

  @Override
  protected String getVarBinaryType() {
    return "RAW(12)";
  }

  // Oracle does not provide a BOOLEAN type.
  @Override
  protected boolean supportsBoolean() {
    return false;
  }

  // Oracle does not provide a BIGINT type.
  @Override
  protected boolean supportsBigInt() {
    return false;
  }

  // Oracle does not provide a TINYINT type.
  @Override
  protected boolean supportsTinyInt() {
    return false;
  }

  // Oracle does not provide a LONGVARCHAR type.
  @Override
  protected boolean supportsLongVarChar() {
    return false;
  }

  // Oracle does not provide a TIME type. We test DATE and TIMESTAMP
  @Override
  protected boolean supportsTime() {
    return false;
  }

  @Override
  protected String getDateInsertStr(String dateStr) {
    return "TO_DATE(" + dateStr + ", 'YYYY-MM-DD')";
  }

  @Override
  protected String getTimestampInsertStr(String tsStr) {
    return "TO_TIMESTAMP(" + tsStr + ", 'YYYY-MM-DD HH24:MI:SS.FF')";
  }

  @Override
  protected String getDateDbOutput(String asInserted) {
    // DATE is actually a TIMESTAMP in Oracle; add a time component.
    return asInserted + " 00:00:00.0";
  }

  @Override
  protected String getDateSeqOutput(String asInserted) {
    return getDateDbOutput(asInserted);
  }

  @Override
  protected String getFixedCharDbOut(int fieldWidth, String asInserted) {
    return padString(fieldWidth, asInserted);
  }

  @Override
  protected String getFixedCharSeqOut(int fieldWidth, String asInserted) {
    return padString(fieldWidth, asInserted);
  }

  @Override
  protected String getRealDbOutput(String realAsInserted) {
    return realAsInserted;
  }

  @Override
  protected String getRealSeqOutput(String realAsInserted) {
    return realAsInserted;
  }

  @Override
  protected String getFloatDbOutput(String floatAsInserted) {
    return floatAsInserted;
  }

  @Override
  protected String getFloatSeqOutput(String floatAsInserted) {
    return floatAsInserted;
  }

  @Override
  protected String getDoubleDbOutput(String doubleAsInserted) {
    return doubleAsInserted;
  }

  @Override
  protected String getDoubleSeqOutput(String doubleAsInserted) {
    return doubleAsInserted;
  }

  @Override
  protected String getVarBinarySeqOutput(String asInserted) {
    return toLowerHexString(asInserted);
  }

  @Override
  protected String getBlobInsertStr(String blobData) {
    // Oracle wants blob data encoded as hex (e.g. '01fca3b5').

    StringBuilder sb = new StringBuilder();
    sb.append("'");

    Formatter fmt = new Formatter(sb);
    try {
      for (byte b : blobData.getBytes("UTF-8")) {
        fmt.format("%02X", b);
      }
    } catch (UnsupportedEncodingException uee) {
      // Should not happen; Java always supports UTF-8.
      fail("Could not get utf-8 bytes for blob string");
      return null;
    }
    sb.append("'");
    return sb.toString();
  }

  // Disable this test since Oracle isn't ANSI compliant.
  @Override
  public void testEmptyStringCol() {
    this.skipped = true;
    LOG.info(
        "Oracle treats empty strings as null (non-ANSI compliant). Skipping.");
  }

  // Date and timestamp output values seem to be formatted in multiple
  // different ways depending on which Oracle JDBC driver subversion is used.
  // We override the test to control the expected output. We actually accept
  // one of multiple different output results -- if it fails one output
  // format, we check the other. Success on either is success for the whole
  // test. Because of this, we can't just use a single getTimestampDbOutput()
  // method.

  @Override
  public void testTimestamp1() {
    try {
      // Older ojdbc6_g jars succeed with this format.
      verifyType(getTimestampType(),
          getTimestampInsertStr("'2009-04-24 18:24:00'"),
          "2009-4-24 18:24:0. 0",
          "2009-04-24 18:24:00.0");
    } catch (AssertionFailedError afe) {
      // Try the test again with a different timestamp format.
      verifyType(getTimestampType(),
          getTimestampInsertStr("'2009-04-24 18:24:00'"),
          "2009-04-24 18:24:00",
          "2009-04-24 18:24:00.0");
    }
  }

  @Override
  public void testTimestamp2() {
    try {
      LOG.debug("Beginning testTimestamp2");
      verifyType(getTimestampType(),
          getTimestampInsertStr("'2009-04-24 18:24:00.0002'"),
          "2009-4-24 18:24:0. 200000",
          "2009-04-24 18:24:00.0002");
    } catch (AssertionFailedError afe) {
      // Try again with the newer version of the timestamp format.
      // The in-db format (third argument) looks weird to me, like it's losing
      // precision. But the final argument (the to-string version of what we
      // extract) is correct, which is what is most important.
      verifyType(getTimestampType(),
          getTimestampInsertStr("'2009-04-24 18:24:00.0002'"),
          "2009-04-24 18:24:00.2",
          "2009-04-24 18:24:00.0002");
    } finally {
      LOG.debug("End testTimestamp2");
    }
  }

  @Override
  public void testDate1() {
    try {
      verifyType("DATE", getDateInsertStr("'2009-01-12'"),
          "2009-01-12 00:00:00.0",
          getDateSeqOutput("2009-01-12"));
    } catch (AssertionFailedError afe) {
      // If the above format doesn't work, it might be a newer ojdbc jar,
      // which does not append the ".0" on the end.
      verifyType("DATE", getDateInsertStr("'2009-01-12'"),
          "2009-01-12 00:00:00",
          getDateSeqOutput("2009-01-12"));
    }
  }

  @Override
  public void testDate2() {
    try {
      verifyType("DATE", getDateInsertStr("'2009-04-24'"),
          "2009-04-24 00:00:00.0",
          getDateSeqOutput("2009-04-24"));
    } catch (AssertionFailedError afe) {
      verifyType("DATE", getDateInsertStr("'2009-04-24'"),
          "2009-04-24 00:00:00",
          getDateSeqOutput("2009-04-24"));
    }
  }

  public void testRawVal() {
    verifyType("RAW(8)", "'12ABCD'", "12ABCD",
        getVarBinarySeqOutput("12ABCD"), true);
  }
}

