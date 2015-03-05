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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

/**
 * Test the DirectMySQLManager implementation.
 * This differs from MySQLManager only in its importTable() method, which
 * uses mysqldump instead of mapreduce+DBInputFormat.
 *
 * Since this requires a MySQL installation on your local machine to use, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=DirectMySQLTest.
 *
 * You need to put MySQL's Connector/J JDBC driver library into a location
 * where Hadoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * You should also create a database named 'sqooptestdb' and authorize yourself:
 *
 * CREATE DATABASE sqooptestdb;
 * use mysql;
 * GRANT ALL PRIVILEGES ON sqooptestdb.* TO 'yourusername'@'localhost';
 * flush privileges;
 *
 */
public class DirectMySQLTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      DirectMySQLTest.class.getName());

  static final String TABLE_PREFIX = "EMPLOYEES_MYSQL_";

  // instance variables populated during setUp, used during tests
  private DirectMySQLManager manager;

  @Override
  protected String getTablePrefix() {
    return TABLE_PREFIX;
  }

  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(MySQLTestUtils.CONNECT_STRING,
        getTableName());
    options.setUsername(MySQLTestUtils.getCurrentUser());

    LOG.debug("Setting up another DirectMySQLTest: "
        + MySQLTestUtils.CONNECT_STRING);

    manager = new DirectMySQLManager(options);

    Connection connection = null;
    Statement st = null;

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data.
      st.executeUpdate("DROP TABLE IF EXISTS " + getTableName());
      st.executeUpdate("CREATE TABLE " + getTableName() + " ("
          + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
          + "name VARCHAR(24) NOT NULL, "
          + "overly_large_number INT UNSIGNED,"
          + "start_date DATE, "
          + "salary FLOAT, "
          + "dept VARCHAR(32))");

      st.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "NULL,'Aaron',0,'2009-05-14',1000000.00,'engineering')");
      st.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "NULL,'Bob',100,'2009-04-20',400.00,'sales')");
      st.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "NULL,'Fred',4000000000,'2009-01-23',15.00,'marketing')");
      connection.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    }
  }

  @After
  public void tearDown() {
    try {
      Statement stmt = manager.getConnection().createStatement();
      stmt.execute("DROP TABLE " + getTableName());
    } catch(SQLException e) {
      LOG.error("Can't clean up the database:", e);
    }

    super.tearDown();
  }

  private String [] getArgv(boolean mysqlOutputDelims, boolean isDirect,
      String tableName, String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(MySQLTestUtils.CONNECT_STRING);
    if (isDirect) {
      args.add("--direct");
    }
    args.add("--username");
    args.add(MySQLTestUtils.getCurrentUser());
    args.add("--where");
    args.add("id > 1");
    args.add("--num-mappers");
    args.add("1");

    if (mysqlOutputDelims) {
      args.add("--mysql-delimiters");
    }

    if (null != extraArgs) {
      for (String arg : extraArgs) {
        args.add(arg);
      }
    }

    return args.toArray(new String[0]);
  }

  private void doImport(boolean mysqlOutputDelims, boolean isDirect,
      String tableName, String [] expectedResults, String [] extraArgs)
      throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);

    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv(mysqlOutputDelims, isDirect, tableName, extraArgs);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file: " + f, f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      for (String expectedLine : expectedResults) {
        assertEquals(expectedLine, r.readLine());
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  @Test
  public void testDirectBulkImportWithDefaultDelims() throws IOException {
    // no quoting of strings allowed.
    String [] expectedResults = {
      "2,Bob,100,2009-04-20,400,sales",
      "3,Fred,4000000000,2009-01-23,15,marketing",
    };

    doImport(false, true, getTableName(), expectedResults, null);
  }

  @Test
  public void testWithExtraParams() throws IOException {
    // no quoting of strings allowed.
    String [] expectedResults = {
      "2,Bob,100,2009-04-20,400,sales",
      "3,Fred,4000000000,2009-01-23,15,marketing",
    };

    String [] extraArgs = { "--", "--lock-tables" };

    doImport(false, true, getTableName(), expectedResults, extraArgs);
  }

  @Test
  public void testMultiMappers() throws IOException {
    // no quoting of strings allowed.
    String [] expectedResults = {
      "2,Bob,100,2009-04-20,400,sales",
      "3,Fred,4000000000,2009-01-23,15,marketing",
    };

    String [] extraArgs = { "-m", "2" };

    doImport(false, true, getTableName(), expectedResults, extraArgs);
  }

  @Test
  public void testJdbcColumnSubset() throws IOException {
    // Test that column subsets work in JDBC mode.
    LOG.info("Starting JDBC Column Subset test.");

    String [] expectedResults = {
      "2,Bob,400.0",
      "3,Fred,15.0",
    };

    String [] extraArgs = { "--columns", "id,name,salary" };
    doImport(false, false, getTableName(), expectedResults, extraArgs);
  }

  @Test
  public void testDirectColumnSubset() throws IOException {
    // Using a column subset should actually force direct mode off, but this
    // should just warn the user and do a normal import.
    LOG.info("Starting Direct Column Subset test.");

    String [] expectedResults = {
      "2,Bob,400.0",
      "3,Fred,15.0",
    };

    String [] extraArgs = { "--columns", "id,name,salary" };
    doImport(false, true, getTableName(), expectedResults, extraArgs);
  }

  @Test
  public void testDirectBulkImportWithMySQLQuotes() throws IOException {
    // mysql quotes all string-based output.
    String [] expectedResults = {
      "2,'Bob',100,'2009-04-20',400,'sales'",
      "3,'Fred',4000000000,'2009-01-23',15,'marketing'",
    };

    doImport(true, true, getTableName(), expectedResults, null);
  }

  @Test
  public void testMySQLJdbcImport() throws IOException {
    String [] expectedResults = {
      "2,Bob,100,2009-04-20,400.0,sales",
      "3,Fred,4000000000,2009-01-23,15.0,marketing",
    };

    doImport(false, false, getTableName(), expectedResults, null);
  }

  @Test
  public void testJdbcEscapedTableName() throws Exception {
    // Test a JDBC-based import of a table whose name is
    // a reserved sql keyword (and is thus `quoted`)
    final String RESERVED_TABLE_NAME = "TABLE";
    SqoopOptions options = new SqoopOptions(MySQLTestUtils.CONNECT_STRING,
        RESERVED_TABLE_NAME);
    options.setUsername(MySQLTestUtils.getCurrentUser());
    ConnManager mgr = new MySQLManager(options);

    Connection connection = null;
    Statement st = null;

    try {
      connection = mgr.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data.
      st.executeUpdate("DROP TABLE IF EXISTS `" + RESERVED_TABLE_NAME + "`");
      st.executeUpdate("CREATE TABLE `" + RESERVED_TABLE_NAME + "` ("
          + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
          + "name VARCHAR(24) NOT NULL, "
          + "start_date DATE, "
          + "salary FLOAT, "
          + "dept VARCHAR(32))");

      st.executeUpdate("INSERT INTO `" + RESERVED_TABLE_NAME + "` VALUES("
          + "2,'Aaron','2009-05-14',1000000.00,'engineering')");
      st.close();
      connection.commit();

      String [] expectedResults = {
          "2,Aaron,2009-05-14,1000000.0,engineering",
      };

      doImport(false, false, RESERVED_TABLE_NAME, expectedResults, null);

      st = connection.createStatement();
      st.execute("DROP TABLE `" + RESERVED_TABLE_NAME + "`");
    } finally {
      if (null != st) {
        st.close();
      }

      if (null != connection) {
        connection.close();
      }
    }

  }

  @Test
  public void testJdbcEscapedColumnName() throws Exception {
    // Test a JDBC-based import of a table with a column whose name is
    // a reserved sql keyword (and is thus `quoted`).
    final String TABLE_NAME = "mysql_escaped_col_table";
    SqoopOptions options = new SqoopOptions(MySQLTestUtils.CONNECT_STRING,
        TABLE_NAME);
    options.setUsername(MySQLTestUtils.getCurrentUser());
    ConnManager mgr = new MySQLManager(options);

    Connection connection = null;
    Statement st = null;

    try {
      connection = mgr.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data.
      st.executeUpdate("DROP TABLE IF EXISTS " + TABLE_NAME);
      st.executeUpdate("CREATE TABLE " + TABLE_NAME + " ("
          + "id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
          + "`table` VARCHAR(24) NOT NULL, "
          + "`CREATE` DATE, "
          + "salary FLOAT, "
          + "dept VARCHAR(32))");

      st.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES("
          + "2,'Aaron','2009-05-14',1000000.00,'engineering')");
      st.close();
      connection.commit();

      String [] expectedResults = {
          "2,Aaron,2009-05-14,1000000.0,engineering",
      };

      doImport(false, false, TABLE_NAME, expectedResults, null);

      st = connection.createStatement();
      st.execute("DROP TABLE " + TABLE_NAME);
    } finally {
      if (null != st) {
        st.close();
      }

      if (null != connection) {
        connection.close();
      }
    }
  }
}
