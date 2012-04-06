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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Formatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.LobAvroImportTestCase;

/**
 * Tests BLOB/CLOB import for Avro with Oracle Db.
 */
public class OracleLobAvroImportTest extends LobAvroImportTestCase {

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
}
