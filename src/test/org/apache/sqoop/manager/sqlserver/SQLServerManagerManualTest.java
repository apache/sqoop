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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * Test methods of the generic SqlManager implementation.
 */
public class SQLServerManagerManualTest extends TestCase {

  public static final Log LOG = LogFactory.getLog(
    SQLServerManagerManualTest.class.getName());

  /** the name of a table that doesn't exist. */
  static final String MISSING_TABLE = "MISSING_TABLE";

  // instance variables populated during setUp, used during tests
  private HsqldbTestServer testServer;
  private ConnManager manager;

  @Before
  public void setUp() {
    MSSQLTestUtils utils = new MSSQLTestUtils();
    try {
      utils.createTableFromSQL(MSSQLTestUtils.CREATE_TALBE_LINEITEM);
      utils.populateLineItem();
    } catch (SQLException e) {
      LOG.error("Setup fail with SQLException: " + StringUtils.stringifyException(e));
      fail("Setup fail with SQLException: " + e.toString());
    }
    Configuration conf = getConf();
    SqoopOptions opts = getSqoopOptions(conf);
    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();
    opts.setUsername(username);
    opts.setPassword(password);
    opts.setConnectString(getConnectString());
    ConnFactory f = new ConnFactory(conf);
    try {
      this.manager = f.getManager(new JobData(opts, new ImportTool()));
      System.out.println("Manger : " + this.manager);
    } catch (IOException ioe) {
      LOG.error("Setup fail with IOException: " + StringUtils.stringifyException(ioe));
      fail("IOException instantiating manager: "
          + StringUtils.stringifyException(ioe));
    }
  }

  @After
  public void tearDown() {
    try {

      MSSQLTestUtils utils = new MSSQLTestUtils();
      utils.dropTableIfExists("TPCH1M_LINEITEM");
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Test
  public void testListColNames() {
    String[] colNames = manager.getColumnNames(getTableName());
    assertNotNull("manager returned no colname list", colNames);
    assertEquals("Table list should be length 2", 16, colNames.length);
    String[] knownFields = MSSQLTestUtils.getColumns();
    for (int i = 0; i < colNames.length; i++) {
      assertEquals(knownFields[i], colNames[i]);
    }
  }

  @Test
  public void testListColTypes() {
    Map<String, Integer> types = manager.getColumnTypes(getTableName());

    assertNotNull("manager returned no types map", types);
    assertEquals("Map should be size=16", 16, types.size());
    assertEquals(types.get("L_ORDERKEY").intValue(), Types.INTEGER);
    assertEquals(types.get("L_COMMENT").intValue(), Types.VARCHAR);
  }

  @Test
  public void testMissingTableColNames() {
    // SQL Server returns an empty column list which gets translated as a
    // zero length array
    // how ever also check in case it returns null, which is also correct
    String[] colNames = manager.getColumnNames(MISSING_TABLE);
    if (colNames == null) {
      assertNull("No column names should be returned for missing table",
          colNames);
    }
    int numItems = colNames.length;
    assertEquals(0, numItems);
  }

  @Test
  public void testMissingTableColTypes() {
    Map<String, Integer> colTypes = manager.getColumnTypes(MISSING_TABLE);
    assertNull("No column types should be returned for missing table",
        colTypes);
  }

  // constants related to testReadTable()
  static final int EXPECTED_NUM_ROWS = 4;
  static final int EXPECTED_COL1_SUM = 10;
  static final int EXPECTED_COL2_SUM = 14;

  @Test
  public void testReadTable() {
    ResultSet results = null;
    try {
      results = manager.readTable(getTableName(), MSSQLTestUtils
          .getColumns());

      assertNotNull("ResultSet from readTable() is null!", results);

      ResultSetMetaData metaData = results.getMetaData();
      assertNotNull("ResultSetMetadata is null in readTable()", metaData);

      // ensure that we get the correct number of columns back
      assertEquals("Number of returned columns was unexpected!", metaData
          .getColumnCount(), 16);

      // should get back 4 rows. They are:
      // 1 2
      // 3 4
      // 5 6
      // 7 8
      // .. so while order isn't guaranteed, we should get back 16 on the
      // left
      // and 20 on the right.
      int sumCol1 = 0, sumCol2 = 0, rowCount = 0;
      while (results.next()) {
        rowCount++;
        sumCol1 += results.getInt(1);
        sumCol2 += results.getInt(2);
      }

      assertEquals("Expected 4 rows back", EXPECTED_NUM_ROWS, rowCount);
      assertEquals("Expected left sum of 10", EXPECTED_COL1_SUM, sumCol1);
      assertEquals("Expected right sum of 14", EXPECTED_COL2_SUM, sumCol2);
    } catch (SQLException sqlException) {
      LOG.error(StringUtils.stringifyException(sqlException));
      fail("SQL Exception: " + sqlException.toString());
    } finally {
      if (null != results) {
        try {
          results.close();
        } catch (SQLException sqlE) {
          LOG.error(StringUtils.stringifyException(sqlE));
          fail("SQL Exception in ResultSet.close(): "
              + sqlE.toString());
        }
      }

      manager.release();
    }
  }

  @Test
  public void testReadMissingTable() {
    ResultSet results = null;
    try {
      String[] colNames = { "*" };
      results = manager.readTable(MISSING_TABLE, colNames);
      assertNull("Expected null resultset from readTable(MISSING_TABLE)",
          results);
    } catch (SQLException sqlException) {
      // we actually expect this pass.
    } finally {
      if (null != results) {
        try {
          results.close();
        } catch (SQLException sqlE) {
          fail("SQL Exception in ResultSet.close(): "
              + sqlE.toString());
        }
      }

      manager.release();
    }
  }

  @Test
  public void testgetPrimaryKeyFromMissingTable() {
    String primaryKey = manager.getPrimaryKey(MISSING_TABLE);
    assertNull("Expected null pkey for missing table", primaryKey);
  }

  @Test
  public void testgetPrimaryKeyFromTableWithoutKey() {
    String primaryKey = manager.getPrimaryKey(getTableName());
    assertNull("Expected null pkey for table without key", primaryKey);
  }

  // constants for getPrimaryKeyFromTable()
  static final String TABLE_WITH_KEY = "TABLE_WITH_KEY";
  static final String KEY_FIELD_NAME = "KEYFIELD";

  @Test
  public void testgetPrimaryKeyFromTable() {
    // first, create a table with a primary key
    Connection conn = null;
    try {
      conn = getManager().getConnection();
      dropTableIfExists(TABLE_WITH_KEY);
      PreparedStatement statement = conn.prepareStatement("CREATE TABLE "
          + TABLE_WITH_KEY + "(" + KEY_FIELD_NAME
          + " INT NOT NULL PRIMARY KEY, foo INT)",
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.executeUpdate();
      statement.close();

      String primaryKey = getManager().getPrimaryKey(TABLE_WITH_KEY);
      assertEquals("Expected null pkey for table without key",
          primaryKey, KEY_FIELD_NAME);

    } catch (SQLException sqlException) {
      LOG.error(StringUtils.stringifyException(sqlException));
      fail("Could not create table with primary key: "
          + sqlException.toString());
    } finally {
      if (null != conn) {
        try {
          conn.close();
        } catch (SQLException sqlE) {
          LOG.warn("Got SQLException during close: "
              + sqlE.toString());
        }
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
    SqoopOptions opt = new SqoopOptions(conf);
    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();
    SqoopOptions opts = new SqoopOptions(conf);
    opts.setUsername(username);
    opts.setPassword(password);

    return opt;
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

  protected String getTableName() {
    return "tpch1m_lineitem";
  }

  protected ConnManager getManager() {
    return manager;
  }

  protected Configuration getConf() {
    return new Configuration();
  }

}
