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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.sqoop.ConnFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

/**
 * Test the SQLServerManager implementation.
 *
 * This uses JDBC to import data from an SQLServer database into HDFS.
 *
 * Since this requires an SQLServer installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=SQLServerManagerImportManualTest.
 *
 * You need to put SQL Server JDBC driver library (sqljdbc4.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * To set up your test environment:
 *   Install SQL Server Express 2012
 *   Create a database SQOOPTEST
 *   Create a login SQOOPUSER with password PASSWORD and grant all
 *   access for SQOOPTEST to SQOOPUSER.
 */
public class SQLServerManagerImportManualTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
          SQLServerManagerImportManualTest.class.getName());

  static final String HOST_URL = System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");

  static final String DATABASE_NAME = "SQOOPTEST";
  static final String DATABASE_USER = "SQOOPUSER";
  static final String DATABASE_PASSWORD = "PASSWORD";
  static final String SCHEMA_DBO = "dbo";
  static final String DBO_TABLE_NAME = "EMPLOYEES_MSSQL";
  static final String SCHEMA_SCH = "sch";
  static final String SCH_TABLE_NAME = "PRIVATE_TABLE";
  static final String CONNECT_STRING = HOST_URL
              + ";databaseName=" + DATABASE_NAME;

  static final String CONNECTOR_FACTORY = System.getProperty(
      "sqoop.test.msserver.connector.factory",
      ConnFactory.DEFAULT_FACTORY_CLASS_NAMES);

  // instance variables populated during setUp, used during tests
  private SQLServerManager manager;

  private Configuration conf = new Configuration();

  @Override
  protected Configuration getConf() {
    return conf;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(CONNECT_STRING,
      DBO_TABLE_NAME);
    options.setUsername(DATABASE_USER);
    options.setPassword(DATABASE_PASSWORD);

    manager = new SQLServerManager(options);

    createTableAndPopulateData(SCHEMA_DBO, DBO_TABLE_NAME);
    createTableAndPopulateData(SCHEMA_SCH, SCH_TABLE_NAME);

    // To test with Microsoft SQL server connector, copy the connector jar to
    // sqoop.thirdparty.lib.dir and set sqoop.test.msserver.connector.factory
    // to com.microsoft.sqoop.SqlServer.MSSQLServerManagerFactory. By default,
    // the built-in SQL server connector is used.
    conf.setStrings(ConnFactory.FACTORY_CLASS_NAMES_KEY, CONNECTOR_FACTORY);
  }

  public void createTableAndPopulateData(String schema, String table) {
    String fulltableName = manager.escapeObjectName(schema)
      + "." + manager.escapeObjectName(table);

    Connection conn = null;
    Statement stmt = null;

    // Create schema if needed
    try {
      conn = manager.getConnection();
      stmt = conn.createStatement();
      stmt.execute("CREATE SCHEMA " + schema);
    } catch (SQLException sqlE) {
      LOG.info("Can't create schema: " + sqlE.getMessage());
    } finally {
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (Exception ex) {
        LOG.warn("Exception while closing stmt", ex);
      }
    }

    // Drop the existing table, if there is one.
    try {
      conn = manager.getConnection();
      stmt = conn.createStatement();
      stmt.execute("DROP TABLE " + fulltableName);
    } catch (SQLException sqlE) {
      LOG.info("Table was not dropped: " + sqlE.getMessage());
    } finally {
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (Exception ex) {
        LOG.warn("Exception while closing stmt", ex);
      }
    }

   // Create and populate table
   try {
      conn = manager.getConnection();
      conn.setAutoCommit(false);
      stmt = conn.createStatement();

      // create the database table and populate it with data.
      stmt.executeUpdate("CREATE TABLE " + fulltableName + " ("
          + "id INT NOT NULL, "
          + "name VARCHAR(24) NOT NULL, "
          + "salary FLOAT, "
          + "dept VARCHAR(32), "
          + "PRIMARY KEY (id))");

      stmt.executeUpdate("INSERT INTO " + fulltableName + " VALUES("
          + "1,'Aaron', "
          + "1000000.00,'engineering')");
      stmt.executeUpdate("INSERT INTO " + fulltableName + " VALUES("
          + "2,'Bob', "
          + "400.00,'sales')");
      stmt.executeUpdate("INSERT INTO " + fulltableName + " VALUES("
          + "3,'Fred', 15.00,"
          + "'marketing')");
      conn.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: ", sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    } finally {
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (Exception ex) {
        LOG.warn("Exception while closing connection/stmt", ex);
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

  @Test
  public void testImportSimple() throws IOException {
    String [] expectedResults = {
      "1,Aaron,1000000.0,engineering",
      "2,Bob,400.0,sales",
      "3,Fred,15.0,marketing",
    };

    doImportAndVerify(DBO_TABLE_NAME, expectedResults);
  }

  @Test
  public void testImportExplicitDefaultSchema() throws IOException {
    String [] expectedResults = {
      "1,Aaron,1000000.0,engineering",
      "2,Bob,400.0,sales",
      "3,Fred,15.0,marketing",
    };

    String[] extraArgs = new String[] {"--schema", SCHEMA_DBO};

    doImportAndVerify(DBO_TABLE_NAME, expectedResults, extraArgs);
  }

  @Test
  public void testImportDifferentSchema() throws IOException {
    String [] expectedResults = {
      "1,Aaron,1000000.0,engineering",
      "2,Bob,400.0,sales",
      "3,Fred,15.0,marketing",
    };

    String[] extraArgs = new String[] {"--schema", SCHEMA_SCH};

    doImportAndVerify(SCH_TABLE_NAME, expectedResults, extraArgs);
  }

  @Test
  public void testImportTableHints() throws IOException {
    String [] expectedResults = {
      "1,Aaron,1000000.0,engineering",
      "2,Bob,400.0,sales",
      "3,Fred,15.0,marketing",
    };

    String[] extraArgs = new String[] {"--table-hints", "NOLOCK"};
    doImportAndVerify(DBO_TABLE_NAME, expectedResults, extraArgs);
  }

  @Test
  public void testImportTableHintsMultiple() throws IOException {
    String [] expectedResults = {
      "1,Aaron,1000000.0,engineering",
      "2,Bob,400.0,sales",
      "3,Fred,15.0,marketing",
    };

    String[] extraArgs = new String[] {"--table-hints", "NOLOCK,NOWAIT"};
    doImportAndVerify(DBO_TABLE_NAME, expectedResults, extraArgs);
  }

  private String [] getArgv(String tableName, String ... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--username");
    args.add(DATABASE_USER);
    args.add("--password");
    args.add(DATABASE_PASSWORD);
    args.add("--num-mappers");
    args.add("1");

    if (extraArgs.length > 0) {
      args.add("--");
      for (String arg : extraArgs) {
        args.add(arg);
      }
    }

    return args.toArray(new String[0]);
  }

  private void doImportAndVerify(String tableName,
                                 String [] expectedResults,
                                 String ... extraArgs) throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);
    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv(tableName, extraArgs);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
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
}
