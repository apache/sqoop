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
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ExportJobConfiguration;
import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;

public class TestExportInitializer extends TestCase {

  private final String tableName;
  private final String tableSql;
  private final String tableColumns;

  private GenericJdbcExecutor executor;

  public TestExportInitializer() {
    tableName = getClass().getSimpleName().toUpperCase();
    tableSql = "INSERT INTO " + tableName + " VALUES (?,?,?)";
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

    ExportJobConfiguration jobConf = new ExportJobConfiguration();

    MutableContext context = new MutableMapContext();

    Initializer initializer = new GenericJdbcExportInitializer();
    initializer.initialize(context, connConf, jobConf);

    verifyResult(context,
        "INSERT INTO " + executor.delimitIdentifier(tableName)
            + " VALUES (?,?,?)",
        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE + tableName);
  }

  public void testTableNameWithTableColumns() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    connConf.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connectionString = GenericJdbcTestConstants.URL;
    connConf.tableName = tableName;
    connConf.columns = tableColumns;

    ExportJobConfiguration jobConf = new ExportJobConfiguration();

    MutableContext context = new MutableMapContext();

    Initializer initializer = new GenericJdbcExportInitializer();
    initializer.initialize(context, connConf, jobConf);

    verifyResult(context,
        "INSERT INTO " + executor.delimitIdentifier(tableName)
            + " (" + tableColumns + ") VALUES (?,?)",
        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE + tableName);
  }

  public void testTableSql() throws Exception {
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    connConf.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connConf.connectionString = GenericJdbcTestConstants.URL;
    connConf.sql = tableSql;

    ExportJobConfiguration jobConf = new ExportJobConfiguration();

    MutableContext context = new MutableMapContext();

    Initializer initializer = new GenericJdbcExportInitializer();
    initializer.initialize(context, connConf, jobConf);

    verifyResult(context,
        "INSERT INTO " + executor.delimitIdentifier(tableName)
            + " VALUES (?,?,?)",
        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE
            + GenericJdbcConnectorConstants.DEFAULT_DATADIR);
  }

  private void verifyResult(MutableContext context,
      String dataSql, String inputDirectory) {
    assertEquals(dataSql, context.getString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL));
    assertEquals(inputDirectory, context.getString(
        Constants.JOB_ETL_INPUT_DIRECTORY));
  }
}
