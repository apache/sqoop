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
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestMultiCols;

/**
 * Test multiple columns SQL Server.
 */
public class SQLServerMultiColsManualTest extends TestMultiCols {

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

  public void testMixed4() {
    // Overridden to bypass test case invalid for MSSQL server
  }

  public void testMixed5() {
    // Overridden to bypass test case invalid for MSSQL server
  }

  public void testMixed6() {
    // Overridden to bypass test case invalid for MSSQL server
  }

  public void testSkipFirstCol() {
    // Overridden to bypass test case invalid for MSSQL server
  }

  public void testSkipSecondCol() {
    // Overridden to bypass test case invalid for MSSQL server
  }

  public void testSkipThirdCol() {
    // Overridden to bypass test case invalid for MSSQL server
  }

}
