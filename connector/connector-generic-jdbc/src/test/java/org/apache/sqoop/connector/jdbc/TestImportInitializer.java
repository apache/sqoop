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

import java.sql.Types;
import java.util.Hashtable;

import junit.framework.TestCase;

import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.MutableContext;
import org.apache.sqoop.job.etl.Options;
import org.junit.Test;

public class TestImportInitializer extends TestCase {

  private final String tableName;
  private final String tableSql;
  private final String tableColumns;

  private GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 101;

  public TestImportInitializer() {
    tableName = getClass().getSimpleName();
    tableSql = "SELECT * FROM \"" + tableName + "\" WHERE ${CONDITIONS}";
    tableColumns = "ICOL,VCOL";
  }

  @Override
  public void setUp() {
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.DRIVER,
        GenericJdbcTestConstants.URL, null, null);

    if (!executor.existTable(tableName)) {
      executor.executeUpdate("CREATE TABLE "
          + executor.delimitIdentifier(tableName)
          + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20))");

      for (int i = 0; i < NUMBER_OF_ROWS; i++) {
        int value = START + i;
        String sql = "INSERT INTO " + executor.delimitIdentifier(tableName)
            + " VALUES(" + value + ", " + value + ", '" + value + "')";
        executor.executeUpdate(sql);
      }
    }
  }

  @Override
  public void tearDown() {
    executor.close();
  }

  @Test
  public void testTableName() throws Exception {
    DummyOptions options = new DummyOptions();
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER,
        GenericJdbcTestConstants.DRIVER);
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING,
        GenericJdbcTestConstants.URL);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_NAME,
        tableName);

    DummyContext context = new DummyContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.run(context, options);

    verifyResult(context,
        "SELECT * FROM " + executor.delimitIdentifier(tableName)
            + " WHERE ${CONDITIONS}",
        "ICOL,DCOL,VCOL",
        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE + tableName,
        "ICOL",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf(START+NUMBER_OF_ROWS-1));
  }

  @Test
  public void testTableNameWithTableColumns() throws Exception {
    DummyOptions options = new DummyOptions();
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER,
        GenericJdbcTestConstants.DRIVER);
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING,
        GenericJdbcTestConstants.URL);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_NAME,
        tableName);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_COLUMNS,
        tableColumns);

    DummyContext context = new DummyContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.run(context, options);

    verifyResult(context,
        "SELECT ICOL,VCOL FROM " + executor.delimitIdentifier(tableName)
            + " WHERE ${CONDITIONS}",
        tableColumns,
        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE + tableName,
        "ICOL",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf(START+NUMBER_OF_ROWS-1));
  }

  @Test
  public void testTableSql() throws Exception {
    DummyOptions options = new DummyOptions();
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER,
        GenericJdbcTestConstants.DRIVER);
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING,
        GenericJdbcTestConstants.URL);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_SQL,
        tableSql);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_PCOL,
        "DCOL");

    DummyContext context = new DummyContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.run(context, options);

    verifyResult(context,
        "SELECT * FROM " + executor.delimitIdentifier(tableName)
            + " WHERE ${CONDITIONS}",
        "ICOL,DCOL,VCOL",
        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE
            + GenericJdbcConnectorConstants.DEFAULT_DATADIR,
        "DCOL",
        String.valueOf(Types.DOUBLE),
        String.valueOf((double)START),
        String.valueOf((double)(START+NUMBER_OF_ROWS-1)));
  }

  @Test
  public void testTableSqlWithTableColumns() throws Exception {
    DummyOptions options = new DummyOptions();
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER,
        GenericJdbcTestConstants.DRIVER);
    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING,
        GenericJdbcTestConstants.URL);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_SQL,
        tableSql);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_COLUMNS,
        tableColumns);
    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_PCOL,
        "DCOL");

    DummyContext context = new DummyContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.run(context, options);

    verifyResult(context,
        "SELECT SQOOP_SUBQUERY_ALIAS.ICOL,SQOOP_SUBQUERY_ALIAS.VCOL FROM "
            + "(SELECT * FROM " + executor.delimitIdentifier(tableName)
            + " WHERE ${CONDITIONS}) SQOOP_SUBQUERY_ALIAS",
        tableColumns,
        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE
            + GenericJdbcConnectorConstants.DEFAULT_DATADIR,
        "DCOL",
        String.valueOf(Types.DOUBLE),
        String.valueOf((double)START),
        String.valueOf((double)(START+NUMBER_OF_ROWS-1)));
  }

  private void verifyResult(DummyContext context,
      String dataSql, String fieldNames, String outputDirectory,
      String partitionColumnName, String partitionColumnType,
      String partitionMinValue, String partitionMaxValue) {
    assertEquals(dataSql, context.getString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL));
    assertEquals(fieldNames, context.getString(
        Constants.JOB_ETL_FIELD_NAMES));
    assertEquals(outputDirectory, context.getString(
        Constants.JOB_ETL_OUTPUT_DIRECTORY));

    assertEquals(partitionColumnName, context.getString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME));
    assertEquals(partitionColumnType, context.getString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE));
    assertEquals(partitionMinValue, context.getString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE));
    assertEquals(partitionMaxValue, context.getString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE));
  }

  public class DummyOptions implements Options {
    Hashtable<String, String> store = new Hashtable<String, String>();

    public void setOption(String key, String value) {
      store.put(key, value);
    }

    @Override
    public String getOption(String key) {
      return store.get(key);
    }
  }

  public class DummyContext implements MutableContext {
    Hashtable<String, String> store = new Hashtable<String, String>();

    @Override
    public String getString(String key) {
      return store.get(key);
    }

    @Override
    public void setString(String key, String value) {
      store.put(key, value);
    }
  }

}
