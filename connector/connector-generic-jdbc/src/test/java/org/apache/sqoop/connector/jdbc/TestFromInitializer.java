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
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Text;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestFromInitializer {

  private final String schemaName;
  private final String tableName;
  private final String schemalessTableName;
  private final String tableSql;
  private final String schemalessTableSql;
  private final List<String> tableColumns;
  private final String testUser;

  private GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 101;

  public TestFromInitializer() {
    schemaName = getClass().getSimpleName().toUpperCase() + "SCHEMA";
    tableName = getClass().getSimpleName().toUpperCase() + "TABLEWITHSCHEMA";
    schemalessTableName = getClass().getSimpleName().toUpperCase() + "TABLE";
    tableSql = "SELECT * FROM " + schemaName + "." + tableName + " WHERE ${CONDITIONS}";
    schemalessTableSql = "SELECT * FROM " + schemalessTableName + " WHERE ${CONDITIONS}";
    testUser = "test_user";
    tableColumns = new LinkedList<>();
    tableColumns.add("ICOL");
    tableColumns.add("VCOL");
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.LINK_CONFIGURATION);

    String fullTableName = executor.encloseIdentifier(schemaName) + "." + executor.encloseIdentifier(tableName);
    if (!executor.existTable(tableName)) {
      executor.executeUpdate("CREATE SCHEMA " + executor.encloseIdentifier(schemaName));
      executor.executeUpdate("CREATE TABLE "
          + fullTableName
          + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20))");

      for (int i = 0; i < NUMBER_OF_ROWS; i++) {
        int value = START + i;
        String sql = "INSERT INTO " + fullTableName
            + " VALUES(" + value + ", " + value + ", '" + value + "')";
        executor.executeUpdate(sql);
      }
    }

    fullTableName = executor.encloseIdentifier(schemalessTableName);
    if (!executor.existTable(schemalessTableName)) {
      executor.executeUpdate("CREATE TABLE "
          + fullTableName
          + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20))");

      for (int i = 0; i < NUMBER_OF_ROWS; i++) {
        int value = START + i;
        String sql = "INSERT INTO " + fullTableName
            + " VALUES(" + value + ", " + value + ", '" + value + "')";
        executor.executeUpdate(sql);
      }
    }
  }

  /**
   * Return Schema representation for the testing table.
   *
   * @param name Name that should be used for the generated schema.
   * @return
   */
  public Schema getSchema(String name) {
    return new Schema(name)
      .addColumn(new FixedPoint("ICOL", 4L, true))
      .addColumn(new FloatingPoint("DCOL", 8L))
      .addColumn(new Text("VCOL"))
    ;
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    executor.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableName() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.tableName = schemalessTableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT * FROM " + executor.encloseIdentifier(schemalessTableName) + " WHERE ${CONDITIONS}",
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf(START+NUMBER_OF_ROWS-1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalTableNameFullRange() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.tableName = schemalessTableName;
    jobConfig.incrementalRead.checkColumn = "ICOL";
    jobConfig.incrementalRead.lastValue = "-51";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT * FROM " + executor.encloseIdentifier(schemalessTableName) + " WHERE ${CONDITIONS}",
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf(START+NUMBER_OF_ROWS-1));

    assertEquals(String.valueOf(START+NUMBER_OF_ROWS-1), context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_LAST_INCREMENTAL_VALUE));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalTableNameFromZero() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.tableName = schemalessTableName;
    jobConfig.incrementalRead.checkColumn = "ICOL";
    jobConfig.incrementalRead.lastValue = "0";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT * FROM " + executor.encloseIdentifier(schemalessTableName) + " WHERE ${CONDITIONS}",
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(1),
        String.valueOf(START+NUMBER_OF_ROWS-1));

    assertEquals(String.valueOf(START+NUMBER_OF_ROWS-1), context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_LAST_INCREMENTAL_VALUE));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableNameWithTableColumns() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.tableName = schemalessTableName;
    jobConfig.fromJobConfig.columnList = tableColumns;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT \"ICOL\", \"VCOL\" FROM " + executor.encloseIdentifier(schemalessTableName) + " WHERE ${CONDITIONS}",
        "\"" + StringUtils.join(tableColumns, "\", \"") + "\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf(START+NUMBER_OF_ROWS-1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableSql() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.sql = schemalessTableSql;
    jobConfig.fromJobConfig.partitionColumn = "DCOL";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT * FROM " + schemalessTableName  + " WHERE ${CONDITIONS}",
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"DCOL\"",
        String.valueOf(Types.DOUBLE),
        String.valueOf((double)START),
        String.valueOf((double)(START+NUMBER_OF_ROWS-1)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalTableSqlFullRange() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.sql = schemalessTableSql;
    jobConfig.fromJobConfig.partitionColumn = "ICOL";
    jobConfig.incrementalRead.checkColumn = "ICOL";
    jobConfig.incrementalRead.lastValue = "-51";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT * FROM " + schemalessTableName + " WHERE ${CONDITIONS}",
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf((START+NUMBER_OF_ROWS-1)));
    assertEquals(String.valueOf(START+NUMBER_OF_ROWS-1), context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_LAST_INCREMENTAL_VALUE));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalTableSqlFromZero() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.sql = schemalessTableSql;
    jobConfig.fromJobConfig.partitionColumn = "ICOL";
    jobConfig.incrementalRead.checkColumn = "ICOL";
    jobConfig.incrementalRead.lastValue = "0";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT * FROM " + schemalessTableName + " WHERE ${CONDITIONS}",
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(1),
        String.valueOf((START+NUMBER_OF_ROWS-1)));
    assertEquals(String.valueOf(START+NUMBER_OF_ROWS-1), context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_LAST_INCREMENTAL_VALUE));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableNameWithSchema() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    String fullTableName = executor.encloseIdentifiers(schemaName, tableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.schemaName = schemaName;
    jobConfig.fromJobConfig.tableName = tableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT * FROM " + fullTableName + " WHERE ${CONDITIONS}",
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf(START+NUMBER_OF_ROWS-1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableNameWithTableColumnsWithSchema() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    String fullTableName = executor.encloseIdentifiers(schemaName, tableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.schemaName = schemaName;
    jobConfig.fromJobConfig.tableName = tableName;
    jobConfig.fromJobConfig.columnList = tableColumns;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        "SELECT \"ICOL\", \"VCOL\" FROM " + fullTableName + " WHERE ${CONDITIONS}",
        "\"" + StringUtils.join(tableColumns, "\", \"") + "\"",
        "\"ICOL\"",
        String.valueOf(Types.INTEGER),
        String.valueOf(START),
        String.valueOf(START+NUMBER_OF_ROWS-1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableSqlWithSchema() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.schemaName = schemaName;
    jobConfig.fromJobConfig.sql = tableSql;
    jobConfig.fromJobConfig.partitionColumn = "DCOL";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context,
        tableSql,
        "\"ICOL\", \"DCOL\", \"VCOL\"",
        "\"DCOL\"",
        String.valueOf(Types.DOUBLE),
        String.valueOf((double)START),
        String.valueOf((double)(START+NUMBER_OF_ROWS-1)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetSchemaForTable() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    String fullTableName = executor.encloseIdentifiers(schemaName, tableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.schemaName = schemaName;
    jobConfig.fromJobConfig.tableName = tableName;
    jobConfig.fromJobConfig.partitionColumn = "DCOL";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);
    Schema schema = initializer.getSchema(initializerContext, linkConfig, jobConfig);
    assertEquals(getSchema(fullTableName), schema);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetSchemaForSql() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.fromJobConfig.schemaName = schemaName;
    jobConfig.fromJobConfig.sql = tableSql;
    jobConfig.fromJobConfig.partitionColumn = "DCOL";

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context, testUser);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcFromInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);
    Schema schema = initializer.getSchema(initializerContext, linkConfig, jobConfig);
    assertEquals(getSchema("Query"), schema);
  }

  /**
   * Asserts expected content inside the generated context.
   *
   * @param context Context that we're validating against
   * @param dataSql Expected SQL fragment
   * @param fieldNames All detected field names, they need to be properly escaped
   * @param partitionColumnName Partition column name, it needs to be properly escaped
   * @param partitionColumnType Partition column type
   * @param partitionMinValue Minimal value for partitioning
   * @param partitionMaxValue Maximal value for partitioning
   */
  private void verifyResult(MutableContext context, String dataSql, String fieldNames, String partitionColumnName, String partitionColumnType, String partitionMinValue, String partitionMaxValue) {
    assertEquals(context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL), dataSql);
    assertEquals(context.getString(Constants.JOB_ETL_FIELD_NAMES), fieldNames);

    assertEquals(context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME), partitionColumnName);
    assertEquals(context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE), partitionColumnType);
    assertEquals(context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE), partitionMinValue);
    assertEquals(context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE), partitionMaxValue);
  }
}
