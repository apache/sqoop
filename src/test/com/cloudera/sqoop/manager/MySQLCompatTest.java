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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.ManagerCompatTestCase;

/**
 * Test the basic mysql connection manager with the various column types.
 */
public class MySQLCompatTest extends ManagerCompatTestCase {

  public static final Log LOG = LogFactory.getLog(
      MySQLCompatTest.class.getName());

  @Override
  protected Log getLogger() {
    return LOG;
  }

  @Override
  protected String getDbFriendlyName() {
    return "MySQL";
  }

  @Override
  protected String getConnectString() {
    return MySQLTestUtils.CONNECT_STRING;
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    opts.setUsername(MySQLTestUtils.getCurrentUser());
    return opts;

  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    Connection conn = getManager().getConnection();
    PreparedStatement statement = conn.prepareStatement(
        "DROP TABLE IF EXISTS " + table,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  @Override
  protected String getLongVarCharType() {
    return "MEDIUMTEXT";
  }

  @Override
  protected String getTimestampType() {
    // return a nullable timestamp type.
    return "TIMESTAMP NULL";
  }

  @Override
  protected String getClobType() {
    return "MEDIUMTEXT";
  }

  @Override
  protected String getBlobType() {
    return "MEDIUMBLOB";
  }

  @Override
  protected String getRealSeqOutput(String realAsInserted) {
    return withDecimalZero(realAsInserted);
  }

  @Override
  protected String getFloatSeqOutput(String floatAsInserted) {
    return withDecimalZero(floatAsInserted);
  }

  @Override
  protected String getDoubleSeqOutput(String doubleAsInserted) {
    return withDecimalZero(doubleAsInserted);
  }

  @Override
  protected String getTimestampSeqOutput(String tsAsInserted) {
    // We trim timestamps to exactly one tenth of a second.
    if ("null".equals(tsAsInserted)) {
      return tsAsInserted;
    }

    int dotPos = tsAsInserted.indexOf(".");
    if (-1 == dotPos) {
      return tsAsInserted + ".0";
    } else {
      return tsAsInserted.substring(0, dotPos + 2);
    }
  }

  @Override
  protected String getNumericSeqOutput(String numAsInserted) {
    // We always pad to exactly the number of digits in
    // getNumericDecPartDigits().

    int totalDecPartSize = getNumericDecPartDigits();
    int numPad; // number of digits to pad by.

    int dotPos =  numAsInserted.indexOf(".");
    if (-1 == dotPos) {
      numAsInserted = numAsInserted + ".";
      numPad = totalDecPartSize;
    } else {
      int existingDecimalSize = numAsInserted.length() - dotPos;
      numPad = totalDecPartSize - existingDecimalSize;
    }

    if (numPad < 0) {
      // We actually have to trim the value.
      return numAsInserted.substring(0, numAsInserted.length() + numPad + 1);
    } else {
      String zeros = "";
      for (int i = 0; i < numPad; i++) {
        zeros =  zeros + "0";
      }
      return numAsInserted + zeros;
    }
  }

  @Override
  protected String getDecimalSeqOutput(String numAsInserted) {
    return getNumericSeqOutput(numAsInserted);
  }

  @Test
  public void testYear() {
    verifyType("YEAR", "2012", "2012");
  }
}

