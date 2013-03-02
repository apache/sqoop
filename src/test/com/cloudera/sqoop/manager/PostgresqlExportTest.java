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

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ExportJobTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 */
public class PostgresqlExportTest extends ExportJobTestCase {
  public static final Log LOG = LogFactory.getLog(
      PostgresqlExportTest.class.getName());

  static final String HOST_URL = System.getProperty(
    "sqoop.test.postgresql.connectstring.host_url",
    "jdbc:postgresql://localhost/");
  static final String DATABASE_USER = System.getProperty(
    "sqoop.test.postgresql.connectstring.username",
    "sqooptest");
  static final String DATABASE_NAME = System.getProperty(
    "sqoop.test.postgresql.connectstring.database",
    "sqooptest");
  static final String PASSWORD = System.getProperty(
    "sqoop.test.postgresql.connectstring.password");

  static final String TABLE_NAME = "EMPLOYEES_PG";
  static final String PROCEDURE_NAME = "INSERT_AN_EMPLOYEE";
  static final String STAGING_TABLE_NAME = "STAGING";
  static final String SCHEMA_PUBLIC = "public";
  static final String SCHEMA_SPECIAL = "special";
  static final String CONNECT_STRING = HOST_URL + DATABASE_NAME;

  protected Connection connection;

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Before
  public void setUp() {
    super.setUp();

    LOG.debug("Setting up postgresql test: " + CONNECT_STRING);

    try {
      connection = DriverManager.getConnection(CONNECT_STRING, DATABASE_USER, PASSWORD);
      connection.setAutoCommit(false);
    } catch (SQLException ex) {
      LOG.error("Can't create connection", ex);
      throw new RuntimeException(ex);
    }

    createTable(TABLE_NAME, SCHEMA_PUBLIC);
    createTable(STAGING_TABLE_NAME, SCHEMA_PUBLIC);
    createTable(TABLE_NAME, SCHEMA_SPECIAL);
    createTable(STAGING_TABLE_NAME, SCHEMA_SPECIAL);
    createProcedure(PROCEDURE_NAME, SCHEMA_PUBLIC);

    LOG.debug("setUp complete.");
  }

  @Override
  public void tearDown() {
    super.tearDown();

    try {
      connection.close();
    } catch (SQLException e) {
      LOG.error("Ignoring exception in tearDown", e);
    }
  }

  private interface CreateIt {
    void createIt(
        Statement st,
        String fullName,
        ConnManager manager) throws SQLException;
  }

  private void createTable(String tableName, String schema) {
    CreateIt createIt = new CreateIt() {
      @Override
      public void createIt(
          Statement st,
          String fullName,
          ConnManager manager) throws SQLException {
        st.executeUpdate("CREATE TABLE " + fullName + " ("
          + manager.escapeColName("id") + " INT NOT NULL PRIMARY KEY, "
          + manager.escapeColName("name") + " VARCHAR(24) NOT NULL, "
          + manager.escapeColName("start_date") + " DATE, "
          + manager.escapeColName("salary") + " FLOAT, "
          + manager.escapeColName("dept") + " VARCHAR(32))");
      }
    };
    create(tableName, "TABLE", schema, createIt);
  }

  private void createProcedure(String procedureName, String schema) {
    CreateIt createIt = new CreateIt() {
      @Override
      public void createIt(
          Statement st,
          String fullName,
          ConnManager manager) throws SQLException {
        st.executeUpdate("CREATE OR REPLACE FUNCTION " + fullName + " ("
          + "IN " + manager.escapeColName("id") + " INT,"
          + "IN " + manager.escapeColName("name") + " VARCHAR(24),"
          + "IN " + manager.escapeColName("start_date") + " DATE,"
          + "IN " + manager.escapeColName("salary") + " FLOAT,"
          + "IN " + manager.escapeColName("dept") + " VARCHAR(32)"
          + ") "
          + "RETURNS VOID "
          + "AS $$ "
          + "BEGIN "
          + "INSERT INTO "
          + escapeTableOrSchemaName(SCHEMA_PUBLIC)
          + "."
          + escapeTableOrSchemaName(TABLE_NAME)
          + " ("
          + manager.escapeColName("id")
          +", "
          + manager.escapeColName("name")
          +", "
          + manager.escapeColName("start_date")
          +", "
          + manager.escapeColName("salary")
          +", "
          + manager.escapeColName("dept")
          + ") VALUES ("
          + manager.escapeColName("id")
          +", "
          + manager.escapeColName("name")
          +", "
          + manager.escapeColName("start_date")
          +", "
          + manager.escapeColName("salary")
          +", "
          + manager.escapeColName("dept")
          + ");"
          + "END;"
          + "$$ LANGUAGE plpgsql;");
      }
    };
    create(procedureName, "FUNCTION", schema, createIt);
  }

  private void create(
      String name,
      String type,
      String schema,
      CreateIt createIt) {
    SqoopOptions options = new SqoopOptions(CONNECT_STRING, name);
    options.setUsername(DATABASE_USER);

    ConnManager manager = null;
    Statement st = null;

    try {
      manager = new PostgresqlManager(options);
      st = connection.createStatement();

      // Create schema if not exists in dummy way (always create and ignore
      // errors.
      try {
        st.executeUpdate("CREATE SCHEMA " + escapeTableOrSchemaName(schema));
        connection.commit();
      } catch (SQLException e) {
        LOG.info("Couldn't create schema " + schema + " (is o.k. as long as"
          + "the schema already exists.", e);
        connection.rollback();
      }

      String fullTableName = escapeTableOrSchemaName(schema)
        + "." + escapeTableOrSchemaName(name);

      try {
        // Try to remove the table first. DROP TABLE IF EXISTS didn't
        // get added until pg 8.3, so we just use "DROP TABLE" and ignore
        // any exception here if one occurs.
        st.executeUpdate("DROP " + type + " " + fullTableName);
      } catch (SQLException e) {
        LOG.info("Couldn't drop "
            + type.toLowerCase()
            + " " +fullTableName
            + " (ok)",
          e);
        // Now we need to reset the transaction.
        connection.rollback();
      }

      createIt.createIt(st, fullTableName, manager);

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

        if (null != manager) {
          manager.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }

    LOG.debug("setUp complete.");
  }

  private String [] getArgv(boolean useTable,
                            String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    if (useTable) {
      args.add("--table");
      args.add(TABLE_NAME);
    } else {
      args.add("--call");
      args.add(PROCEDURE_NAME);
    }
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
      "2,Bob,2009-04-20,400,sales",
      "3,Fred,2009-01-23,15,marketing",
    });

    runExport(getArgv(true));

    assertRowCount(2, escapeTableOrSchemaName(TABLE_NAME), connection);
  }

  public void testExportUsingProcedure() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
        "2,Bob,2009-04-20,400,sales",
        "3,Fred,2009-01-23,15,marketing",
    });

    runExport(getArgv(false));

    assertRowCount(2, escapeTableOrSchemaName(TABLE_NAME), connection);
  }

  public void testExportStaging() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,2009-04-20,400,sales",
      "3,Fred,2009-01-23,15,marketing",
    });

    String[] extra = new String[] {"--staging-table", STAGING_TABLE_NAME, };

    runExport(getArgv(true, extra));

    assertRowCount(2, escapeTableOrSchemaName(TABLE_NAME), connection);
  }

  public void testExportDirect() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,2009-04-20,400,sales",
      "3,Fred,2009-01-23,15,marketing",
    });

    String[] extra = new String[] {"--direct"};

    runExport(getArgv(true, extra));

    assertRowCount(2, escapeTableOrSchemaName(TABLE_NAME), connection);
  }

  public void testExportCustomSchema() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,2009-04-20,400,sales",
      "3,Fred,2009-01-23,15,marketing",
    });

    String[] extra = new String[] {"--",
      "--schema",
      SCHEMA_SPECIAL,
    };

    runExport(getArgv(true, extra));

    assertRowCount(2,
      escapeTableOrSchemaName(SCHEMA_SPECIAL)
        + "." + escapeTableOrSchemaName(TABLE_NAME),
      connection);
  }

  public void testExportCustomSchemaStaging() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,2009-04-20,400,sales",
      "3,Fred,2009-01-23,15,marketing",
    });

    String[] extra = new String[] {
      "--staging-table",
      STAGING_TABLE_NAME,
      "--",
      "--schema",
      SCHEMA_SPECIAL,
    };

    runExport(getArgv(true, extra));

    assertRowCount(2,
      escapeTableOrSchemaName(SCHEMA_SPECIAL)
        + "." + escapeTableOrSchemaName(TABLE_NAME),
      connection);
  }

  public void testExportCustomSchemaStagingClear()
    throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,2009-04-20,400,sales",
      "3,Fred,2009-01-23,15,marketing",
    });

    String[] extra = new String[] {
      "--staging-table",
      STAGING_TABLE_NAME,
      "--clear-staging-table",
      "--",
      "--schema",
      SCHEMA_SPECIAL,
    };

    runExport(getArgv(true, extra));

    assertRowCount(2,
      escapeTableOrSchemaName(SCHEMA_SPECIAL)
        + "." + escapeTableOrSchemaName(TABLE_NAME),
      connection);
  }

  public void testExportCustomSchemaDirect() throws IOException, SQLException {
    createTestFile("inputFile", new String[] {
      "2,Bob,2009-04-20,400,sales",
      "3,Fred,2009-01-23,15,marketing",
    });

    String[] extra = new String[] {
      "--direct",
      "--",
      "--schema",
      SCHEMA_SPECIAL,
    };

    runExport(getArgv(true, extra));

    assertRowCount(2,
      escapeTableOrSchemaName(SCHEMA_SPECIAL)
        + "." + escapeTableOrSchemaName(TABLE_NAME),
      connection);
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

  public String escapeTableOrSchemaName(String tableName) {
    return "\"" + tableName + "\"";
  }
}
