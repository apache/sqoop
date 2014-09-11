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

import junit.framework.TestCase;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.ValidationResult;
import org.apache.sqoop.validation.ValidationRunner;

public class TestToInitializer extends TestCase {
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

  @Override
  public void setUp() {
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.DRIVER,
        GenericJdbcTestConstants.URL, null, null);

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

  @Override
  public void tearDown() {
    executor.close();
  }

  @SuppressWarnings("unchecked")
  public void testTableName() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemalessTableName);

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.tableName = schemalessTableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);

    verifyResult(context, "INSERT INTO " + fullTableName + " VALUES (?,?,?)");
  }

  @SuppressWarnings("unchecked")
  public void testTableNameWithTableColumns() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemalessTableName);

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.tableName = schemalessTableName;
    jobConf.toTable.columns = tableColumns;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);

    verifyResult(context, "INSERT INTO " + fullTableName + " (" + tableColumns + ") VALUES (?,?)");
  }

  @SuppressWarnings("unchecked")
  public void testTableSql() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.sql = schemalessTableSql;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);

    verifyResult(context, "INSERT INTO " + executor.delimitIdentifier(schemalessTableName) + " VALUES (?,?,?)");
  }

  @SuppressWarnings("unchecked")
  public void testTableNameWithSchema() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.schemaName = schemaName;
    jobConf.toTable.tableName = tableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);

    verifyResult(context, "INSERT INTO " + fullTableName + " VALUES (?,?,?)");
  }

  @SuppressWarnings("unchecked")
  public void testTableNameWithTableColumnsWithSchema() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    String fullTableName = executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.schemaName = schemaName;
    jobConf.toTable.tableName = tableName;
    jobConf.toTable.columns = tableColumns;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);

    verifyResult(context, "INSERT INTO " + fullTableName + " (" + tableColumns + ") VALUES (?,?)");
  }

  @SuppressWarnings("unchecked")
  public void testTableSqlWithSchema() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.schemaName = schemaName;
    jobConf.toTable.sql = tableSql;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);

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

  public void testNonExistingStageTable() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.tableName = schemalessTableName;
    jobConf.toTable.stageTableName = stageTableName;

    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    try {
      initializer.initialize(initializerContext, connConf, jobConf);
      fail("Initialization should fail for non-existing stage table.");
    } catch(SqoopException se) {
      //expected
    }
  }

  @SuppressWarnings("unchecked")
  public void testNonEmptyStageTable() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    String fullStageTableName = executor.delimitIdentifier(stageTableName);

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.tableName = schemalessTableName;
    jobConf.toTable.stageTableName = stageTableName;
    createTable(fullStageTableName);
    executor.executeUpdate("INSERT INTO " + fullStageTableName +
      " VALUES(1, 1.1, 'one')");
    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    try {
      initializer.initialize(initializerContext, connConf, jobConf);
      fail("Initialization should fail for non-empty stage table.");
    } catch(SqoopException se) {
      //expected
    }
  }

  @SuppressWarnings("unchecked")
  public void testClearStageTableValidation() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    //specifying clear stage table flag without specifying name of
    // the stage table
    jobConf.toTable.tableName = schemalessTableName;
    jobConf.toTable.clearStageTable = false;
    ValidationRunner validationRunner = new ValidationRunner();
    ValidationResult result = validationRunner.validate(jobConf);
    assertEquals("User should not specify clear stage table flag without " +
      "specifying name of the stage table",
      Status.UNACCEPTABLE,
        result.getStatus());
    assertTrue(result.getMessages().containsKey(
      "toTable"));

    jobConf.toTable.clearStageTable = true;
    result = validationRunner.validate(jobConf);
    assertEquals("User should not specify clear stage table flag without " +
      "specifying name of the stage table",
      Status.UNACCEPTABLE,
        result.getStatus());
    assertTrue(result.getMessages().containsKey(
      "toTable"));
  }

  @SuppressWarnings("unchecked")
  public void testStageTableWithoutTable() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    //specifying stage table without specifying table name
    jobConf.toTable.stageTableName = stageTableName;
    jobConf.toTable.sql = "";

    ValidationRunner validationRunner = new ValidationRunner();
    ValidationResult result = validationRunner.validate(jobConf);
    assertEquals("Stage table name cannot be specified without specifying " +
      "table name", Status.UNACCEPTABLE, result.getStatus());
    assertTrue(result.getMessages().containsKey(
      "toTable"));
  }

  @SuppressWarnings("unchecked")
  public void testClearStageTable() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    String fullStageTableName = executor.delimitIdentifier(stageTableName);

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.tableName = schemalessTableName;
    jobConf.toTable.stageTableName = stageTableName;
    jobConf.toTable.clearStageTable = true;
    createTable(fullStageTableName);
    executor.executeUpdate("INSERT INTO " + fullStageTableName +
      " VALUES(1, 1.1, 'one')");
    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);
    assertEquals("Stage table should have been cleared", 0,
      executor.getTableRowCount(stageTableName));
  }

  @SuppressWarnings("unchecked")
  public void testStageTable() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();

    String fullStageTableName = executor.delimitIdentifier(stageTableName);

    connConf.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connection.connectionString = GenericJdbcTestConstants.URL;
    jobConf.toTable.tableName = schemalessTableName;
    jobConf.toTable.stageTableName = stageTableName;
    createTable(fullStageTableName);
    MutableContext context = new MutableMapContext();
    InitializerContext initializerContext = new InitializerContext(context);

    @SuppressWarnings("rawtypes")
    Initializer initializer = new GenericJdbcToInitializer();
    initializer.initialize(initializerContext, connConf, jobConf);

    verifyResult(context, "INSERT INTO " + fullStageTableName +
      " VALUES (?,?,?)");
  }
}
