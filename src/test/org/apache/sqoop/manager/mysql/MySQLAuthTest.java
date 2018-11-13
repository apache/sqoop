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

package org.apache.sqoop.manager.mysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.testcategories.thirdpartytest.MysqlTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.sqoop.manager.DirectMySQLManager;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testutil.CommonArgs;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test authentication and remote access to direct mysqldump-based imports.
 *
 * Since this requires a MySQL installation with a properly configured database and user, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=MySQLAuthTest
 *
 * You need to put MySQL's Connector/J JDBC driver library into a location
 * where Hadoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * If you don't have a database and a user which can be used by Sqoop, you can create them using
 * the following MySQL commands:
 *
 * CREATE DATABASE sqooppasstest;
 * use mysql;
 * GRANT ALL PRIVILEGES on sqooppasstest.* TO 'sqooptest'@'localhost'
 *     IDENTIFIED BY '12345';
 * flush privileges;
 *
 * <br/>
 *
 * Ant command for running this test case: <br/>
 * ant clean test
 * -Dsqoop.thirdparty.lib.dir=mysql_driver_dir
 * -Dsqoop.test.mysql.connectstring.host_url=jdbc:mysql://mysql_server_address/
 * -Dsqoop.test.mysql.username=sqooptest
 * -Dsqoop.test.mysql.password=12345
 * -Dsqoop.test.mysql.databasename=sqooppasstest
 * -Dtestcase=MySQLAuthTest
 *
 */
@Category(MysqlTest.class)
public class MySQLAuthTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      MySQLAuthTest.class.getName());

  private MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();

  private List<String> createdTableNames = new ArrayList<>();

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Before
  public void setUp() {
    super.setUp();
    SqoopOptions options = new SqoopOptions(mySQLTestUtils.getMySqlConnectString(),
        getTableName());
    options.setUsername(mySQLTestUtils.getUserName());
    options.setPassword(mySQLTestUtils.getUserPass());

    LOG.debug("Setting up another MySQLAuthTest: " + mySQLTestUtils.getMySqlConnectString());

    setManager(new DirectMySQLManager(options));
  }

  @After
  public void tearDown() {
    dropAllCreatedTables();
    super.tearDown();
  }

  private String [] getArgv(boolean includeHadoopFlags,
      boolean useDirect, String connectString, String tableName) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(connectString);
    if (useDirect) {
      args.add("--direct");
    }
    args.add("--username");
    args.add(mySQLTestUtils.getUserName());
    args.add("--password");
    args.add(mySQLTestUtils.getUserPass());
    args.add("--mysql-delimiters");
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  /**
   * Connect to a db and ensure that password-based authentication
   * succeeds.
   */
  @Test
  public void testAuthAccess() {
    createAndPopulateAuthTable();
    String [] argv = getArgv(true, true, mySQLTestUtils.getMySqlConnectString(), getTableName());
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, getTableName());
    Path filePath = new Path(tablePath, "part-m-00000");

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      assertEquals("1,'Aaron'", r.readLine());
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  @Test
  public void testZeroTimestamp() throws IOException, SQLException {
    // MySQL timestamps can hold values whose range causes problems
    // for java.sql.Timestamp. The MySQLManager adds settings to the
    // connect string which configure the driver's handling of
    // zero-valued timestamps. Check that all of these modifications
    // to the connect string are successful.

    // A connect string with a null 'query' component.
    doZeroTimestampTest(0, true, mySQLTestUtils.getMySqlConnectString());

    // A connect string with a zero-length query component.
    doZeroTimestampTest(1, true, mySQLTestUtils.getMySqlConnectString() + "?");

    // A connect string with another argument
    doZeroTimestampTest(2, true, mySQLTestUtils.getMySqlConnectString() + "?connectTimeout=0");
    doZeroTimestampTest(3, true, mySQLTestUtils.getMySqlConnectString() + "?connectTimeout=0&");

    // A connect string with the zero-timestamp behavior already
    // configured.
    doZeroTimestampTest(4, true, mySQLTestUtils.getMySqlConnectString()
        + "?zeroDateTimeBehavior=convertToNull");

    // And finally, behavior already configured in such a way as to
    // cause the timestamp import to fail.
    doZeroTimestampTest(5, false, mySQLTestUtils.getMySqlConnectString()
        + "?zeroDateTimeBehavior=exception");
  }

  public void doZeroTimestampTest(int testNum, boolean expectSuccess,
      String connectString) throws IOException, SQLException {

    LOG.info("Beginning zero-timestamp test #" + testNum);

    final String tableName = "mysqlTimestampTable" + Integer.toString(testNum);

    createAndPopulateZeroTimestampTable(tableName);

    // Run the import.
    String [] argv = getArgv(true, false, connectString, tableName);
    try {
      runImport(argv);
    } catch (Exception e) {
      if (expectSuccess) {
        // This is unexpected. rethrow.
        throw new RuntimeException(e);
      } else {
        // We expected an error.
        LOG.info("Got exception running import (expected). msg: " + e);
      }
    }

    // Make sure the result file is there.
    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);
    Path filePath = new Path(tablePath, "part-m-00000");

    File f = new File(filePath.toString());
    if (expectSuccess) {
      assertTrue("Could not find imported data file", f.exists());
      BufferedReader r = new BufferedReader(new InputStreamReader(
          new FileInputStream(f)));
      assertEquals("1,null", r.readLine());
      IOUtils.closeStream(r);
    } else {
      assertFalse("Imported data when expected failure", f.exists());
    }
  }

  private void createAndPopulateZeroTimestampTable(String tableName) {
    String[] colNames = { "id", "ts" };
    String[] colTypes = { "INT NOT NULL PRIMARY KEY AUTO_INCREMENT", "TIMESTAMP NOT NULL" };
    String[] colValues = { "NULL", "'0000-00-00 00:00:00.0'" };
    createTableWithColTypesAndNames(tableName, colNames, colTypes, colValues);
    createdTableNames.add(tableName);
  }

  private void dropAllCreatedTables() {
    try {
      for (String createdTableName : createdTableNames) {
        dropTableIfExists(createdTableName);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void createAndPopulateAuthTable() {
    String[] colNames = { "id", "name" };
    String[] colTypes = { "INT NOT NULL PRIMARY KEY AUTO_INCREMENT", "VARCHAR(24) NOT NULL" };
    String[] colValues = { "NULL", "'Aaron'" };

    createTableWithColTypesAndNames(colNames, colTypes, colValues);
    createdTableNames.add(getTableName());
  }

  protected String dropTableIfExistsCommand(String tableName) {
    return String.format("DROP TABLE IF EXISTS %s", tableName);
  }

}
