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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.ConfigValidationRunner;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestToInitializer {
  private final String schemaName;
  private final String tableName;
  private final String schemalessTableName;
  private final String stageTableName;
  private final String tableSql;
  private final String schemalessTableSql;
  private final String tableColumns;

  private GenericJdbcExecutor executor;

  public TestToInitializer() {
    schemaName = getClass().getSimpleName().toUpperCase() + "SCHEMA";
    tableName = getClass().getSimpleName().toUpperCase() + "TABLEWITHSCHEMA";
    schemalessTableName = getClass().getSimpleName().toUpperCase() + "TABLE";
    stageTableName = getClass().getSimpleName().toUpperCase() +
      "_STAGE_TABLE";
    tableSql = "INSERT INTO " + tableName + " VALUES (?,?,?)";
    schemalessTableSql = "INSERT INTO " + schemalessTableName + " VALUES (?,?,?)";
    tableColumns = "ICOL,VCOL";
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.LINK_CONFIG);

    String fullTableName = executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);
    if (!executor.existTable(tableName)) {
      executor.executeUpdate("CREATE SCHEMA " + executor.delimitIdentifier(schemaName));
      executor.executeUpdate("CREATE TABLE " + fullTableName + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20))");
    }

    fullTableName = executor.delimitIdentifier(schemalessTableName);
    if (!executor.existTable(schemalessTableName)) {
      executor.executeUpdate("CREATE TABLE " + fullTableName + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20))");
    }
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    executor.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableName() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemalessTableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.tableName = schemalessTableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context, "INSERT INTO " + fullTableName + " VALUES (?,?,?)");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableNameWithTableColumns() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemalessTableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.tableName = schemalessTableName;
    jobConfig.toJobConfig.columns = tableColumns;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context, "INSERT INTO " + fullTableName + " (" + tableColumns + ") VALUES (?,?)");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableSql() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.sql = schemalessTableSql;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context, "INSERT INTO " + executor.delimitIdentifier(schemalessTableName) + " VALUES (?,?,?)");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableNameWithSchema() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.schemaName = schemaName;
    jobConfig.toJobConfig.tableName = tableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context, "INSERT INTO " + fullTableName + " VALUES (?,?,?)");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableNameWithTableColumnsWithSchema() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.schemaName = schemaName;
    jobConfig.toJobConfig.tableName = tableName;
    jobConfig.toJobConfig.columns = tableColumns;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context, "INSERT INTO " + fullTableName + " (" + tableColumns + ") VALUES (?,?)");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTableSqlWithSchema() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.schemaName = schemaName;
    jobConfig.toJobConfig.sql = tableSql;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context, "INSERT INTO " + executor.delimitIdentifier(tableName) + " VALUES (?,?,?)");
  }

  private void verifyResult(MutableContext context, String dataSql) {
    assertEquals(dataSql, context.getString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_TO_DATA_SQL));
  }

  private void createTable(String tableName) {
    try {
      executor.executeUpdate("DROP TABLE " + tableName);
    } catch(SqoopException e) {
      //Ok to fail as the table might not exist
    }
    executor.executeUpdate("CREATE TABLE " + tableName +
      "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20))");
  }

  @Test
  public void testNonExistingStageTable() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.tableName = schemalessTableName;
    jobConfig.toJobConfig.stageTableName = stageTableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    try {
      initializer.initialize(initializerContext, linkConfig, jobConfig);
      fail("Initialization should fail for non-existing stage table.");
    } catch(SqoopException se) {
      //expected
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNonEmptyStageTable() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    String fullStageTableName = executor.delimitIdentifier(stageTableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.tableName = schemalessTableName;
    jobConfig.toJobConfig.stageTableName = stageTableName;
    createTable(fullStageTableName);
    executor.executeUpdate("INSERT INTO " + fullStageTableName +
      " VALUES(1, 1.1, 'one')");
    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    try {
      initializer.initialize(initializerContext, linkConfig, jobConfig);
      fail("Initialization should fail for non-empty stage table.");
    } catch(SqoopException se) {
      //expected
    }
  }

  @Test
  public void testClearStageTableValidation() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    //specifying clear stage table flag without specifying name of
    // the stage table
    jobConfig.toJobConfig.tableName = schemalessTableName;
    jobConfig.toJobConfig.shouldClearStageTable = false;
    ConfigValidationRunner validationRunner = new ConfigValidationRunner();
    ConfigValidationResult result = validationRunner.validate(jobConfig);
    assertEquals("User should not specify clear stage table flag without " +
      "specifying name of the stage table",
      Status.ERROR,
        result.getStatus());
    assertTrue(result.getMessages().containsKey(
      "toJobConfig"));

    jobConfig.toJobConfig.shouldClearStageTable = true;
    result = validationRunner.validate(jobConfig);
    assertEquals("User should not specify clear stage table flag without " +
      "specifying name of the stage table",
      Status.ERROR,
        result.getStatus());
    assertTrue(result.getMessages().containsKey(
      "toJobConfig"));
  }

  @Test
  public void testStageTableWithoutTable() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    //specifying stage table without specifying table name
    jobConfig.toJobConfig.stageTableName = stageTableName;
    jobConfig.toJobConfig.sql = "";

    ConfigValidationRunner validationRunner = new ConfigValidationRunner();
    ConfigValidationResult result = validationRunner.validate(jobConfig);
    assertEquals("Stage table name cannot be specified without specifying " +
      "table name", Status.ERROR, result.getStatus());
    assertTrue(result.getMessages().containsKey(
      "toJobConfig"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testClearStageTable() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    String fullStageTableName = executor.delimitIdentifier(stageTableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.tableName = schemalessTableName;
    jobConfig.toJobConfig.stageTableName = stageTableName;
    jobConfig.toJobConfig.shouldClearStageTable = true;
    createTable(fullStageTableName);
    executor.executeUpdate("INSERT INTO " + fullStageTableName +
      " VALUES(1, 1.1, 'one')");
    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);
    assertEquals("Stage table should have been cleared", 0,
      executor.getTableRowCount(stageTableName));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStageTable() throws Exception {
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    String fullStageTableName = executor.delimitIdentifier(stageTableName);

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;
    jobConfig.toJobConfig.tableName = schemalessTableName;
    jobConfig.toJobConfig.stageTableName = stageTableName;
    createTable(fullStageTableName);
    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    verifyResult(context, "INSERT INTO " + fullStageTableName +
      " VALUES (?,?,?)");
  }
}
