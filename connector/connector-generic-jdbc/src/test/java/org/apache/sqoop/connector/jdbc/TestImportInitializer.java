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

import junit.framework.TestCase;

import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ImportJobConfiguration;
import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;

public class TestImportInitializer extends TestCase {

  private final String tableName;
  private final String tableSql;
  private final String tableColumns;

  private GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 101;

  public TestImportInitializer() {
    tableName = getClass().getSimpleName().toUpperCase();
    tableSql = "SELECT * FROM " + tableName + " WHERE ${CONDITIONS}";
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

  public void testTableName() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    connConf.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connectionString = GenericJdbcTestConstants.URL;
    connConf.tableName = tableName;

    ImportJobConfiguration jobConf = new ImportJobConfiguration();

    MutableContext context = new MutableMapContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.initialize(context, connConf, jobConf);

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

  public void testTableNameWithTableColumns() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    connConf.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connectionString = GenericJdbcTestConstants.URL;
    connConf.tableName = tableName;
    connConf.columns = tableColumns;

    ImportJobConfiguration jobConf = new ImportJobConfiguration();

    MutableContext context = new MutableMapContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.initialize(context, connConf, jobConf);

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

  public void testTableSql() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    connConf.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connectionString = GenericJdbcTestConstants.URL;
    connConf.sql = tableSql;
    connConf.partitionColumn = "DCOL";

    ImportJobConfiguration jobConf = new ImportJobConfiguration();

    MutableContext context = new MutableMapContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.initialize(context, connConf, jobConf);

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

  public void testTableSqlWithTableColumns() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    connConf.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connectionString = GenericJdbcTestConstants.URL;
    connConf.sql = tableSql;
    connConf.columns = tableColumns;
    connConf.partitionColumn = "DCOL";

    ImportJobConfiguration jobConf = new ImportJobConfiguration();

    MutableContext context = new MutableMapContext();

    Initializer initializer = new GenericJdbcImportInitializer();
    initializer.initialize(context, connConf, jobConf);

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

  private void verifyResult(MutableContext context,
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
}
