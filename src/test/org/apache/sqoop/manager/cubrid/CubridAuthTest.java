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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.CubridManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Test authentication.
 *
 * Since this requires a CUBRID installation on your local machine to use, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=CubridAuthTest
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
public class CubridAuthTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(CubridAuthTest.class
      .getName());

  static final String TABLE_NAME = "employees_cubrid";

  private CubridManager manager;
  private Configuration conf = new Configuration();

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Before
  public void setUp() {
    super.setUp();
    LOG.debug("Setting up another CubridImport test: "
      + CubridTestUtils.getConnectString());
    setUpData(TABLE_NAME, true);
    LOG.debug("setUp complete.");
  }

  public void setUpData(String tableName, boolean nullEntry) {
    SqoopOptions options = new SqoopOptions(
        CubridTestUtils.getConnectString(), TABLE_NAME);
    options.setUsername(CubridTestUtils.getCurrentUser());
    options.setPassword(CubridTestUtils.getPassword());

    LOG.debug("Setting up another CubridImport test: "
      + CubridTestUtils.getConnectString());
    manager = new CubridManager(options);
    Connection connection = null;
    Statement st = null;

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data.
      st.executeUpdate("DROP TABLE IF EXISTS " + TABLE_NAME);
      String sqlStmt = "CREATE TABLE "
          + TABLE_NAME + " ("
          + manager.escapeColName("id")
          + " INT NOT NULL PRIMARY KEY, "
          + manager.escapeColName("name")
          + " VARCHAR(24) NOT NULL);";
      st.executeUpdate(sqlStmt);
      st.executeUpdate("INSERT INTO " + TABLE_NAME
          + " VALUES(1,'Aaron');");
      connection.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    } finally {
      try {
        if (null != st) {
          st.close();
        }

        if (null != connection) {
          connection.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: "
          + sqlE);
      }
    }
  }

  @After
  public void tearDown() {
    super.tearDown();
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  /**
   * Connect to a db and ensure that password-based
   *  authentication succeeds.
   */
  @Test
  public void testAuthAccess() throws IOException {
    SqoopOptions options = new SqoopOptions(conf);
    options.setConnectString(CubridTestUtils.getConnectString());
    options.setUsername(CubridTestUtils.getCurrentUser());
    options.setPassword(CubridTestUtils.getPassword());

    ConnManager mgr = new CubridManager(options);
    String[] tables = mgr.listTables();
    Arrays.sort(tables);
    assertTrue(TABLE_NAME + " is not found!",
        Arrays.binarySearch(tables, TABLE_NAME) >= 0);
  }
}
