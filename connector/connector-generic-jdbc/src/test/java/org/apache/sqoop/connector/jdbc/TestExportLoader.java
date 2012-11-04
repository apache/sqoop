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

import java.sql.ResultSet;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.io.DataReader;
import org.junit.Test;

public class TestExportLoader extends TestCase {

  private final String tableName;

  private GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 101;

  public TestExportLoader() {
    tableName = getClass().getSimpleName();
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
//  public void testInsert() throws Exception {
//    DummyContext context = new DummyContext();
//    context.setString(
//        GenericJdbcConnectorConstants.CONNECTOR_JDBC_DRIVER,
//        GenericJdbcTestConstants.DRIVER);
//    context.setString(
//        GenericJdbcConnectorConstants.CONNECTOR_JDBC_URL,
//        GenericJdbcTestConstants.URL);
//    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
//        "INSERT INTO " + executor.delimitIdentifier(tableName)
//            + " VALUES (?,?,?)");
//
//    Loader loader = new GenericJdbcExportLoader();
//    DummyReader reader = new DummyReader();
//
//    loader.run(context, reader);
//
//    int index = START;
//    ResultSet rs = executor.executeQuery("SELECT * FROM "
//        + executor.delimitIdentifier(tableName) + " ORDER BY ICOL");
//    while (rs.next()) {
//      assertEquals(Integer.valueOf(index), rs.getObject(1));
//      assertEquals(Double.valueOf(index), rs.getObject(2));
//      assertEquals(String.valueOf(index), rs.getObject(3));
//      index++;
//    }
//    assertEquals(NUMBER_OF_ROWS, index-START);
//  }
//
//  public class DummyContext implements MutableContext {
//    HashMap<String, String> store = new HashMap<String, String>();
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
//
//  public class DummyReader extends DataReader {
//    int index = 0;
//
//    @Override
//    public void setFieldDelimiter(char fieldDelimiter) {
//      // do nothing and use default delimiter
//    }
//
//    @Override
//    public Object[] readArrayRecord() {
//      if (index < NUMBER_OF_ROWS) {
//        Object[] array = new Object[] {
//            new Integer(START+index),
//            new Double(START+index),
//            String.valueOf(START+index) };
//        index++;
//        return array;
//      } else {
//        return null;
//      }
//    }
//
//    @Override
//    public String readCsvRecord() {
//      fail("This method should not be invoked.");
//      return null;
//    }
//
//    @Override
//    public Object readContent(int type) {
//      fail("This method should not be invoked.");
//      return null;
//    }
//  }

}
