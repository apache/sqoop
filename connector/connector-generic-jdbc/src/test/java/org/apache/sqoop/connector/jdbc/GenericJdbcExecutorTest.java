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

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfig;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class GenericJdbcExecutorTest {
  private final String table;
  private final String emptyTable;
  private final GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 974;

  public GenericJdbcExecutorTest() {
    table = getClass().getSimpleName().toUpperCase();
    emptyTable = table + "_EMPTY";
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.LINK_CONFIGURATION);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    if(executor.existTable(emptyTable)) {
      executor.executeUpdate("DROP TABLE " + emptyTable);
    }
    executor.executeUpdate("CREATE TABLE "
      + emptyTable + "(ICOL INTEGER PRIMARY KEY, VCOL VARCHAR(20))");

    if(executor.existTable(table)) {
      executor.executeUpdate("DROP TABLE " + table);
    }
    executor.executeUpdate("CREATE TABLE "
      + table + "(ICOL INTEGER PRIMARY KEY, VCOL VARCHAR(20))");

    for (int i = 0; i < NUMBER_OF_ROWS; i++) {
      int value = START + i;
      String sql = "INSERT INTO " + table
        + " VALUES(" + value + ", '" + value + "')";
      executor.executeUpdate(sql);
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
}
