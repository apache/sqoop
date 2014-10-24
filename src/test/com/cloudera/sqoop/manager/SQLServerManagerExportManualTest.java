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

import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.cloudera.sqoop.testutil.ExportJobTestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Please see instructions in SQLServerManagerImportManualTest.
 */
public class SQLServerManagerExportManualTest extends ExportJobTestCase {

    public static final Log LOG = LogFactory.getLog(
      SQLServerManagerExportManualTest.class.getName());

  static final String HOST_URL = System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");

  static final String DATABASE_NAME = "SQOOPTEST";
  static final String DATABASE_USER = "SQOOPUSER";
  static final String DATABASE_PASSWORD = "PASSWORD";
  static final String SCHEMA_DBO = "dbo";
  static final String DBO_TABLE_NAME = "EMPLOYEES_MSSQL";
  static final String DBO_BINARY_TABLE_NAME = "BINARYTYPE_MSSQL";
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
  private Connection conn = null;

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

    Statement stmt = null;

    // Create schema if needed
    try {
      conn = manager.getConnection();
      stmt = conn.createStatement();
      stmt.execute("CREATE SCHEMA " + schema);
      conn.commit();
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
      conn.commit();
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

  public void createSQLServerBinaryTypeTable(String schema, String table) {
    String fulltableName = manager.escapeObjectName(schema)
      + "." + manager.escapeObjectName(table);

    Statement stmt = null;

    // Create schema if needed
    try {
    conn = manager.getConnection();
    stmt = conn.createStatement();
    stmt.execute("CREATE SCHEMA " + schema);
    conn.commit();
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
      conn.commit();
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
          + "id INT PRIMARY KEY, "
          + "b1 BINARY(10), "
          + "b2 VARBINARY(10))");
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
      conn.close();
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  private String [] getArgv(String tableName,
                            String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(tableName);
    args.add("--export-dir");
    args.add(getWarehouseDir());
    args.add("--fields-terminated-by");
    args.add(",");
    args.add("--lines-terminated-by");
    args.add("\\n");
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--username");
    args.add(DATABASE_USER);
    args.add("--password");
    args.add(DATABASE_PASSWORD);
    args.add("-m");
    args.add("1");

    for (String arg : extraArgs) {
      args.add(arg);
    }

    return args.toArray(new String[0]);
  }

  protected void createTestFile(String filename,
                                String[] lines)
                                throws IOException {
    new File(getWarehouseDir()).mkdirs();
    File file = new File(getWarehouseDir() + "/" + filename);
    Writer output = new BufferedWriter(new FileWriter(file));
    for(String line : lines) {
      output.write(line);
      output.write("\n");
    }
    output.close();
  }

  public void testExport() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,400,sales",
      "3,Fred,15,marketing",
    });

    runExport(getArgv(DBO_TABLE_NAME));

    assertRowCount(2, escapeObjectName(DBO_TABLE_NAME), conn);
  }

  public void testExportCustomSchema() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,400,sales",
      "3,Fred,15,marketing",
    });

    String[] extra = new String[] {"--",
      "--schema",
      SCHEMA_SCH,
    };

    runExport(getArgv(SCH_TABLE_NAME, extra));

    assertRowCount(
      2,
      escapeObjectName(SCHEMA_SCH) + "." + escapeObjectName(SCH_TABLE_NAME),
      conn
    );
  }

  public void testExportTableHints() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,400,sales",
      "3,Fred,15,marketing",
    });

    String []extra = new String[] {"--", "--table-hints",
      "ROWLOCK",
    };
    runExport(getArgv(DBO_TABLE_NAME, extra));
    assertRowCount(2, escapeObjectName(DBO_TABLE_NAME), conn);
  }

  public void testExportTableHintsMultiple() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,400,sales",
      "3,Fred,15,marketing",
    });

    String []extra = new String[] {"--", "--table-hints",
      "ROWLOCK,NOWAIT",
    };
    runExport(getArgv(DBO_TABLE_NAME, extra));
    assertRowCount(2, escapeObjectName(DBO_TABLE_NAME), conn);
  }

  public void testSQLServerBinaryType() throws IOException, SQLException {
    createSQLServerBinaryTypeTable(SCHEMA_DBO, DBO_BINARY_TABLE_NAME);
    createTestFile("inputFile", new String[] {
      "1,73 65 63 72 65 74 00 00 00 00,73 65 63 72 65 74"
    });
    String[] expectedContent = {"73656372657400000000", "736563726574"};
    runExport(getArgv(DBO_BINARY_TABLE_NAME));
    assertRowCount(1, escapeObjectName(DBO_BINARY_TABLE_NAME), conn);
    checkSQLBinaryTableContent(expectedContent, escapeObjectName(DBO_BINARY_TABLE_NAME), conn);
  }

  /** Make sure mixed update/insert export work correctly. */
  public void testUpsertTextExport() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,400,sales",
      "3,Fred,15,marketing",
    });
    // first time will be insert.
    runExport(getArgv(SCH_TABLE_NAME, "--update-key", "id",
              "--update-mode", "allowinsert"));
    // second time will be update.
    runExport(getArgv(SCH_TABLE_NAME, "--update-key", "id",
              "--update-mode", "allowinsert"));
    assertRowCount(2, escapeObjectName(SCH_TABLE_NAME), conn);
  }

  public static void checkSQLBinaryTableContent(String[] expected, String tableName, Connection connection){
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery("SELECT TOP 1 [b1], [b2] FROM " + tableName);
      rs.next();
      assertEquals(expected[0], rs.getString("b1"));
      assertEquals(expected[1], rs.getString("b2"));
    } catch (SQLException e) {
        LOG.error("Can't verify table content", e);
        fail();
    } finally {
      try {
        connection.commit();

        if (stmt != null) {
          stmt.close();
        }
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException ex) {
        LOG.info("Ignored exception in finally block.");
      }
    }
  }

  public static void assertRowCount(long expected,
                                    String tableName,
                                    Connection connection) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery("SELECT count(*) FROM " + tableName);

      rs.next();

      assertEquals(expected, rs.getLong(1));
    } catch (SQLException e) {
      LOG.error("Can't verify number of rows", e);
      fail();
    } finally {
      try {
        connection.commit();

        if (stmt != null) {
          stmt.close();
        }
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException ex) {
        LOG.info("Ignored exception in finally block.");
      }
    }
  }

  public String escapeObjectName(String objectName) {
    return "[" + objectName + "]";
  }
}
