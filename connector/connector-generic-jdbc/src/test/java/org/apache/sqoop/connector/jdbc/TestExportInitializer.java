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

import java.util.Hashtable;

import junit.framework.TestCase;

import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;
//import org.apache.sqoop.job.etl.MutableContext;
//import org.apache.sqoop.job.etl.Options;
import org.junit.Test;

public class TestExportInitializer extends TestCase {

  private final String tableName;
  private final String tableSql;
  private final String tableColumns;

  private GenericJdbcExecutor executor;

  public TestExportInitializer() {
    tableName = getClass().getSimpleName();
    tableSql = "INSERT INTO \"" + tableName + "\" VALUES (?,?,?)";
    tableColumns = "ICOL,VCOL";
  }

  public void testVoid() { }

//  @Override
//  public void setUp() {
//    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.DRIVER,
//        GenericJdbcTestConstants.URL, null, null);
//
//    if (!executor.existTable(tableName)) {
//      executor.executeUpdate("CREATE TABLE "
//          + executor.delimitIdentifier(tableName)
//          + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20))");
//    }
//  }
//
//  @Override
//  public void tearDown() {
//    executor.close();
//  }
//
//  @Test
//  public void testTableName() throws Exception {
//    DummyOptions options = new DummyOptions();
//    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER,
//        GenericJdbcTestConstants.DRIVER);
//    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING,
//        GenericJdbcTestConstants.URL);
//    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_NAME,
//        tableName);
//
//    DummyContext context = new DummyContext();
//
//    Initializer initializer = new GenericJdbcExportInitializer();
//    initializer.run(context, options);
//
//    verifyResult(context,
//        "INSERT INTO " + executor.delimitIdentifier(tableName)
//            + " VALUES (?,?,?)",
//        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE + tableName);
//  }
//
//  @Test
//  public void testTableNameWithTableColumns() throws Exception {
//    DummyOptions options = new DummyOptions();
//    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER,
//        GenericJdbcTestConstants.DRIVER);
//    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING,
//        GenericJdbcTestConstants.URL);
//    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_NAME,
//        tableName);
//    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_COLUMNS,
//        tableColumns);
//
//    DummyContext context = new DummyContext();
//
//    Initializer initializer = new GenericJdbcExportInitializer();
//    initializer.run(context, options);
//
//    verifyResult(context,
//        "INSERT INTO " + executor.delimitIdentifier(tableName)
//            + " (" + tableColumns + ") VALUES (?,?)",
//        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE + tableName);
//  }
//
//  @Test
//  public void testTableSql() throws Exception {
//    DummyOptions options = new DummyOptions();
//    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER,
//        GenericJdbcTestConstants.DRIVER);
//    options.setOption(GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING,
//        GenericJdbcTestConstants.URL);
//    options.setOption(GenericJdbcConnectorConstants.INPUT_TBL_SQL,
//        tableSql);
//
//    DummyContext context = new DummyContext();
//
//    Initializer initializer = new GenericJdbcExportInitializer();
//    initializer.run(context, options);
//
//    verifyResult(context,
//        "INSERT INTO " + executor.delimitIdentifier(tableName)
//            + " VALUES (?,?,?)",
//        GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE
//            + GenericJdbcConnectorConstants.DEFAULT_DATADIR);
//  }
//
//  private void verifyResult(DummyContext context,
//      String dataSql, String inputDirectory) {
//    assertEquals(dataSql, context.getString(
//        GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL));
//    assertEquals(inputDirectory, context.getString(
//        Constants.JOB_ETL_INPUT_DIRECTORY));
//  }
//
//  public class DummyOptions implements Options {
//    Hashtable<String, String> store = new Hashtable<String, String>();
//
//    public void setOption(String key, String value) {
//      store.put(key, value);
//    }
//
//    @Override
//    public String getOption(String key) {
//      return store.get(key);
//    }
//  }
//
//  public class DummyContext implements MutableContext {
//    Hashtable<String, String> store = new Hashtable<String, String>();
//
//    @Override
//    public String getString(String key) {
//      return store.get(key);
//    }
//
//    @Override
//    public void setString(String key, String value) {
//      store.put(key, value);
//    }
//  }

}
