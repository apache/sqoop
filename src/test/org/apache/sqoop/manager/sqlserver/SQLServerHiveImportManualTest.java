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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hive.TestHiveImport;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * Test import to Hive from SQL Server.
 */
public class SQLServerHiveImportManualTest extends TestHiveImport {

  public void setUp() {
    super.setUp();

  }

  protected boolean useHsqldbTestServer() {
    return false;
  }

  protected String getConnectString() {
    return System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");
  }

  //SQL Server pads out
  protected String[] getTypesNewLineTest() {
    String[] types = { "VARCHAR(32)", "INTEGER", "VARCHAR(64)" };
    return types;
  }

  /**
   * Drop a table if it already exists in the database.
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

  SqoopOptions getSqoopOptions(String[] args, SqoopTool tool) {
    SqoopOptions opts = null;
    try {
      opts = tool.parseArguments(args, null, null, true);
      String username = MSSQLTestUtils.getDBUserName();
      String password = MSSQLTestUtils.getDBPassWord();
      opts.setUsername(username);
      opts.setPassword(password);

    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      fail("Invalid options: " + e.toString());
    }

    return opts;
  }

  protected String[] getArgv(boolean includeHadoopFlags, String[] moreArgs) {
    ArrayList<String> args = new ArrayList<String>();
    System.out.println("Ovverdien getArgv is called..");
    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    if (null != moreArgs) {
      for (String arg : moreArgs) {
        args.add(arg);
      }
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--hive-import");
    String[] colNames = getColNames();
    if (null != colNames) {
      args.add("--split-by");
      args.add(colNames[0]);
    } else {
      fail("Could not determine column names.");
    }

    args.add("--num-mappers");
    args.add("1");

    for (String a : args) {
      LOG.debug("ARG : " + a);
    }

    return args.toArray(new String[0]);
  }

  protected String[] getCodeGenArgs() {
    ArrayList<String> args = new ArrayList<String>();

    args.add("--table");
    args.add(getTableName());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--hive-import");

    return args.toArray(new String[0]);
  }
}
