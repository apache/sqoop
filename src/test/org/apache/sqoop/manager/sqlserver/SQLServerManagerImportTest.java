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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.sqoop.ConnFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.SQLServerManager;
import org.apache.sqoop.testcategories.thirdpartytest.SqlServerTest;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.BaseSqoopTestCase;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.apache.sqoop.util.ExpectedLogMessage;
import org.apache.sqoop.util.FileListing;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
/**
 * Test the SQLServerManager implementation.
 *
 * This uses JDBC to import data from an SQLServer database into HDFS.
 *
 * Since this requires an SQLServer installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=SQLServerManagerImportTest or -Dthirdparty=true.
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
@Category(SqlServerTest.class)
public class SQLServerManagerImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
          SQLServerManagerImportTest.class.getName());

  static final String HOST_URL = System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");
  static final String DATABASE_NAME = System.getProperty(
      "sqoop.test.sqlserver.database",
      "sqooptest");
  static final String DATABASE_USER = System.getProperty(
      "ms.sqlserver.username",
      "sqoopuser");
  static final String DATABASE_PASSWORD = System.getProperty(
      "ms.sqlserver.password",
      "password");

  static final String SCHEMA_DBO = "dbo";
  static final String DBO_TABLE_NAME = "EMPLOYEES_MSSQL";
  static final String SCHEMA_SCH = "sch";
  static final String SCH_TABLE_NAME = "PRIVATE_TABLE";
  static final String CONNECT_STRING = HOST_URL + ";databaseName=" + DATABASE_NAME;

  static final String CONNECTOR_FACTORY = System.getProperty(
      "sqoop.test.msserver.connector.factory",
      ConnFactory.DEFAULT_FACTORY_CLASS_NAMES);

  // instance variables populated during setUp, used during tests
  private SQLServerManager manager;

  private Configuration conf = new Configuration();

  private Connection conn = null;

  public static final String[] EXPECTED_RESULTS = new String[]{
      "1,Aaron,1000000.0,engineering",
      "2,Bob,400.0,sales",
      "3,Fred,15.0,marketing",
  };

  @Parameters(name = "Builder: {0}, Table: {1}")
  public static Iterable<? extends Object> testConfigurations() {
    ArgumentArrayBuilder builderForTableImportWithExplicitSchema = getArgsBuilderForTableImport().withToolOption("schema", SCHEMA_DBO);
    return Arrays.asList(
        new Object[] { getArgsBuilderForQueryImport(), DBO_TABLE_NAME },
        new Object[] { getArgsBuilderForTableImport(), DBO_TABLE_NAME },
        new Object[] { getArgsBuilderForDifferentSchemaTableImport(), SCH_TABLE_NAME },
        new Object[] { builderForTableImportWithExplicitSchema, DBO_TABLE_NAME }
    );
  }

  private final ArgumentArrayBuilder builder;
  private final String tableName;

  public SQLServerManagerImportTest(ArgumentArrayBuilder builder, String tableName) {
    this.builder = new ArgumentArrayBuilder().with(builder);
    this.tableName = tableName;
  }

  @Override
  protected Configuration getConf() {
    return conf;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  private String getDropTableStatement(String schema, String tableName) {
    return "DROP TABLE IF EXISTS " + manager.escapeObjectName(schema)
        + "." + manager.escapeObjectName(tableName);
  }

  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(CONNECT_STRING, DBO_TABLE_NAME);
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
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(getDropTableStatement(SCHEMA_DBO, DBO_TABLE_NAME));
      stmt.executeUpdate(getDropTableStatement(SCHEMA_SCH, SCH_TABLE_NAME));
    } catch (SQLException e) {
      LOG.error("Can't clean up the database:", e);
    }

    super.tearDown();
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Rule
  public ExpectedLogMessage logMessage = new ExpectedLogMessage();

  @Test
  public void testImportSimple() throws IOException {
    doImportAndVerify(builder, tableName);
  }

  @Test
  public void testImportTableHints() throws IOException {
    builder.withToolOption("table-hints", "NOLOCK");
    doImportAndVerify(builder, tableName);
  }

  @Test
  public void testImportTableHintsMultiple() throws IOException {
    builder.withToolOption("table-hints", "NOLOCK,NOWAIT");
    doImportAndVerify(builder, tableName);
  }

  @Test
  public void testImportTableResilient() throws IOException {
    logMessage.expectWarn("Sqoop will use resilient operations! In case of import, the split-by column also has to be specified, unique, and in ascending order.");
    builder.withToolOption("resilient");
    doImportAndVerify(builder, tableName);
  }

  /**
   * The resilient option was named non-resilient before, but got renamed.
   * This test is here to ensure backward compatibility in the sense that
   * using the non-resilient option won't break any job.
   *
   * @throws IOException
   */
  @Test
  public void testImportTableNonResilient() throws IOException {
    builder.withToolOption("non-resilient");
    doImportAndVerify(builder, tableName);
  }

  private static ArgumentArrayBuilder getArgsBuilder() {
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    return builder.withCommonHadoopFlags(true)
        .withOption("connect", CONNECT_STRING)
        .withOption("username", DATABASE_USER)
        .withOption("password", DATABASE_PASSWORD)
        .withOption("num-mappers",  "1")
        .withOption("split-by", "id");
  }

  private static ArgumentArrayBuilder getArgsBuilderForTableImport() {
    ArgumentArrayBuilder builder = getArgsBuilder();
    return builder.withCommonHadoopFlags(true)
        .withOption("warehouse-dir", BaseSqoopTestCase.getLocalWarehouseDir())
        .withOption("table", DBO_TABLE_NAME);
  }

  private static ArgumentArrayBuilder getArgsBuilderForQueryImport() {
    ArgumentArrayBuilder builder = getArgsBuilder();
    return builder.withCommonHadoopFlags(true)
        .withOption("query", "SELECT * FROM EMPLOYEES_MSSQL WHERE $CONDITIONS")
        .withOption("target-dir", BaseSqoopTestCase.getLocalWarehouseDir() + "/" + DBO_TABLE_NAME);
  }

  private static ArgumentArrayBuilder getArgsBuilderForDifferentSchemaTableImport() {
    ArgumentArrayBuilder builder = getArgsBuilder();
    return builder.withCommonHadoopFlags(true)
        .withOption("warehouse-dir", BaseSqoopTestCase.getLocalWarehouseDir())
        .withOption("table", SCH_TABLE_NAME)
        .withToolOption("schema", SCHEMA_SCH);
  }

  private void doImportAndVerify(ArgumentArrayBuilder argBuilder,
                                 String tableName) throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);
    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = argBuilder.build();
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
      for (String expectedLine : EXPECTED_RESULTS) {
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
