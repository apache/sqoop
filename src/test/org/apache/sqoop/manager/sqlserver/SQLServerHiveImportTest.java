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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.hive.TestHiveImport;
import org.apache.sqoop.testutil.CommonArgs;
import org.apache.sqoop.tool.SqoopTool;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.fail;

/**
 * Test import to Hive from SQL Server.
 *
 * This uses JDBC to import data from an SQLServer database to HDFS.
 *
 * Since this requires an SQLServer installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=SQLServerHiveImportTest or -Dthirdparty=true.
 *
 * You need to put SQL Server JDBC driver library (sqljdbc4.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons) and set it's path through -Dsqoop.thirdparty.lib.dir.
 *
 * To set up your test environment:
 *   Install SQL Server Express 2012
 *   Create a database SQOOPTEST
 *   Create a login SQOOPUSER with password PASSWORD and grant all
 *   access for SQOOPTEST to SQOOPUSER.
 *   Set these through -Dsqoop.test.sqlserver.connectstring.host_url, -Dsqoop.test.sqlserver.database and
 *   -Dms.sqlserver.password
 */
public class SQLServerHiveImportTest extends TestHiveImport {

  @Before
  public void setUp() {
    super.setUp();
  }

  @After
  public void tearDown() {
    try {
      dropTableIfExists(getTableName());
    } catch (SQLException sqle) {
      LOG.info("Table clean-up failed: " + sqle);
    } finally {
      super.tearDown();
    }
  }

  protected boolean useHsqldbTestServer() {
    return false;
  }

  protected String getConnectString() {
    return MSSQLTestUtils.getDBConnectString();
  }

  //SQL Server pads out
  @Override
  protected String[] getTypes() {
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
    System.out.println("Overridden getArgv is called..");
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
