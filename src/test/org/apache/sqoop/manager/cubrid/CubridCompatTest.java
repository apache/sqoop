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

package org.apache.sqoop.manager.cubrid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.ManagerCompatTestCase;

/**
 * Test the basic Cubrid connection manager with the various column types.
 *
 * Since this requires a CUBRID installation on your local machine to use, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=CubridCompatTest
 *
 * You need to put CUBRID's Connector/J JDBC driver library into a location
 * where Hadoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * To set up your test environment:
 *   Install Cubrid 9.2.2
 *   ref:http://www.cubrid.org/wiki_tutorials/entry/installing-cubrid-on-linux-using-shell-and-rpm
 *   Create a database SQOOPCUBRIDTEST
 *   $cubrid createdb SQOOPCUBRIDTEST en_us.utf8
 *   Start cubrid and database
 *   $cubrid service start
 *   $cubrid server start SQOOPCUBRIDTEST
 *   Create a login SQOOPUSER with password PASSWORD and grant all
 *   $csql -u dba SQOOPCUBRIDTEST
 *   csql>CREATE USER SQOOPUSER password 'PASSWORD';
 */
public class CubridCompatTest extends ManagerCompatTestCase {

  public static final Log LOG = LogFactory.getLog(CubridCompatTest.class
      .getName());

  @Override
  protected Log getLogger() {
    return LOG;
  }

  @Override
  protected String getDbFriendlyName() {
    return "CUBRID";
  }

  @Override
  protected String getConnectString() {
    return CubridTestUtils.getConnectString();
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    opts.setUsername(CubridTestUtils.getCurrentUser());
    opts.setPassword(CubridTestUtils.getPassword());
    return opts;

  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    Connection conn = getManager().getConnection();
    PreparedStatement statement = conn.prepareStatement(
        "DROP TABLE IF EXISTS "
        + table, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  @Override
  protected boolean supportsBoolean() {
    return false;
  }

  @Override
  protected boolean supportsLongVarChar() {
    return false;
  }

  @Override
  protected String getFixedCharSeqOut(int fieldWidth, String asInserted) {
    return padString(fieldWidth, asInserted);
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
  protected String getVarBinaryType() {
    return "BIT VARYING(48)";
  }

  @Override
  protected String getVarBinarySeqOutput(String asInserted) {
    return toLowerHexString(asInserted);
  }

  @Override
  protected String getNumericSeqOutput(String numAsInserted) {
    int totalDecPartSize = getNumericDecPartDigits();
    int numPad; // number of digits to pad by.

    int dotPos = numAsInserted.indexOf(".");
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
        zeros = zeros + "0";
      }
      return numAsInserted + zeros;
    }
  }

  @Override
  protected String getDecimalSeqOutput(String numAsInserted) {
    return getNumericSeqOutput(numAsInserted);
  }
}
