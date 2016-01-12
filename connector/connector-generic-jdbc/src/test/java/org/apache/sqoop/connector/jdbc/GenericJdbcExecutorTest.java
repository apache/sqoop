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
package org.apache.sqoop.connector.jdbc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GenericJdbcExecutorTest {
  private final String table;
  private final String emptyTable;
  private final String schema;
  private final String compoundPrimaryKeyTable;
  private GenericJdbcExecutor executor;

  private static final int START = -10;
  private static final int NUMBER_OF_ROWS = 20;

  public GenericJdbcExecutorTest() {
    table = getClass().getSimpleName().toUpperCase();
    emptyTable = table + "_EMPTY";
    schema = table + "_SCHEMA";
    compoundPrimaryKeyTable = table + "_COMPOUND";
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.LINK_CONFIGURATION);
    executor.executeUpdate("CREATE SCHEMA " + executor.encloseIdentifier(schema));
    executor.executeUpdate("CREATE TABLE " + executor.encloseIdentifier(emptyTable )+ "(ICOL INTEGER PRIMARY KEY, VCOL VARCHAR(20))");
    executor.executeUpdate("CREATE TABLE " + executor.encloseIdentifier(table) + "(ICOL INTEGER PRIMARY KEY, VCOL VARCHAR(20))");
    executor.executeUpdate("CREATE TABLE " + executor.encloseIdentifiers(schema, table) + "(ICOL INTEGER PRIMARY KEY, VCOL VARCHAR(20))");
    executor.executeUpdate("CREATE TABLE " + executor.encloseIdentifier(compoundPrimaryKeyTable) + "(ICOL INTEGER, VCOL VARCHAR(20), PRIMARY KEY(VCOL, ICOL))");

    for (int i = 0; i < NUMBER_OF_ROWS; i++) {
      int value = START + i;
      executor.executeUpdate("INSERT INTO " + executor.encloseIdentifier(table) + " VALUES(" + value + ", '" + value + "')");
      executor.executeUpdate("INSERT INTO " + executor.encloseIdentifiers(schema, table) + " VALUES(" + value + ", '" + value + "')");
    }
  }

  @AfterMethod
  public void tearDown() throws SQLException {
    executor.close();
    try {
      DriverManager.getConnection(GenericJdbcTestConstants.URL_DROP);
    } catch(SQLException e) {
      // Code 8006 means that the database has been successfully drooped
      if(e.getErrorCode() != 45000 && e.getNextException().getErrorCode() == 8006) {
        throw e;
      }

    }
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testUnknownDriver() {
    LinkConfiguration link = new LinkConfiguration();
    link.linkConfig.jdbcDriver = "net.jarcec.driver.MyAwesomeDatabase";
    link.linkConfig.connectionString = "jdbc:awesome:";

    new GenericJdbcExecutor(link);
  }

  @Test
  public void testGetPrimaryKey() {
    assertEquals(executor.getPrimaryKey("non-existing-table"), new String[] {});
    assertEquals(executor.getPrimaryKey("non-existing-schema", "non-existing-table"), new String[] {});
    assertEquals(executor.getPrimaryKey("non-existing-catalog", "non-existing-schema", "non-existing-table"), new String[] {});

    assertEquals(executor.getPrimaryKey(schema, table), new String[] {"ICOL"});
    assertEquals(executor.getPrimaryKey(compoundPrimaryKeyTable), new String[] {"VCOL", "ICOL"});
  }

  @Test(expectedExceptions = SqoopException.class)
  public void TestGetPrimaryKeySameTableInMultipleSchemas() {
    // Same table name exists in two schemas and therefore we should fail here
    executor.getPrimaryKey(table);
  }

  @Test
  public void testExistsTable() {
    assertFalse(executor.existTable("non-existing-table"));
    assertFalse(executor.existTable("non-existing-schema", "non-existing-table"));
    assertFalse(executor.existTable("non-existing-catalog", "non-existing-schema", "non-existing-table"));

    assertTrue(executor.existTable(table));
    assertTrue(executor.existTable(schema, table));
  }

  @Test
  public void testEncloseIdentifier() {
    assertEquals(executor.encloseIdentifier("a"), "\"a\"");
  }

  @Test
  public void testEncloseIdentifiers() {
//    assertEquals(executor.encloseIdentifiers("a"), "\"a\"");
//    assertEquals(executor.encloseIdentifiers(null, "a"), "\"a\"");
//    assertEquals(executor.encloseIdentifiers("a", "b"), "\"a\".\"b\"");
      assertEquals(executor.encloseIdentifiers("a"), "a");
      assertEquals(executor.encloseIdentifiers(null, "a"), "a");
      assertEquals(executor.encloseIdentifiers("a", "b"), "a.b");

  }

  @Test
  public void testColumnList() {
    assertEquals(executor.columnList("a"), "\"a\"");
    assertEquals(executor.columnList("a", "b"), "\"a\", \"b\"");
  }

  @Test
  public void testDeleteTableData() throws Exception {
    executor.deleteTableData(table);
    assertEquals(0, executor.getTableRowCount(table),
            "Table " + table + " is expected to be empty.");
  }

  @Test
  public void testMigrateData() throws Exception {
    assertEquals(0, executor.getTableRowCount(emptyTable),
            "Table " + emptyTable + " is expected to be empty.");
    assertEquals(NUMBER_OF_ROWS, executor.getTableRowCount(table),
            "Table " + table + " is expected to have " + NUMBER_OF_ROWS + " rows.");

    executor.migrateData(table, emptyTable);

    assertEquals(0, executor.getTableRowCount(table),
            "Table " + table + " is expected to be empty.");
    assertEquals(NUMBER_OF_ROWS, executor.getTableRowCount(emptyTable),
            "Table " + emptyTable + " is expected to have " + NUMBER_OF_ROWS + " rows.");
  }

  @Test
  public void testGetTableRowCount() throws Exception {
    assertEquals(NUMBER_OF_ROWS, executor.getTableRowCount(table),
            "Table " + table + " is expected to be empty.");
  }

  @Test
  public void testFetchSize() throws Exception {
    assertEquals((int) GenericJdbcTestConstants.LINK_CONFIGURATION.linkConfig.fetchSize,
      executor.createStatement().getFetchSize());
    assertEquals((int) GenericJdbcTestConstants.LINK_CONFIGURATION.linkConfig.fetchSize,
      executor.prepareStatement("SELECT * FROM " + table).getFetchSize());
  }
}
