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

import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.MutableContext;
import org.apache.sqoop.job.io.DataWriter;
import org.junit.Test;

public class TestImportExtractor extends TestCase {

  private final String tableName;

  private GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 101;

  public TestImportExtractor() {
    tableName = getClass().getSimpleName();
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
  public void testQuery() throws Exception {
    DummyContext context = new DummyContext();
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_DRIVER,
        GenericJdbcTestConstants.DRIVER);
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_URL,
        GenericJdbcTestConstants.URL);
    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
        "SELECT * FROM " + executor.delimitIdentifier(tableName)
            + " WHERE ${CONDITIONS}");

    GenericJdbcImportPartition partition;

    Extractor extractor = new GenericJdbcImportExtractor();
    DummyWriter writer = new DummyWriter();

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-50.0 <= DCOL AND DCOL < -16.6666666666666665");
    extractor.run(context, partition, writer);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-16.6666666666666665 <= DCOL AND DCOL < 16.666666666666667");
    extractor.run(context, partition, writer);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("16.666666666666667 <= DCOL AND DCOL <= 50.0");
    extractor.run(context, partition, writer);
  }

  @Test
  public void testSubquery() throws Exception {
    DummyContext context = new DummyContext();
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_DRIVER,
        GenericJdbcTestConstants.DRIVER);
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_URL,
        GenericJdbcTestConstants.URL);
    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
        "SELECT SQOOP_SUBQUERY_ALIAS.ICOL,SQOOP_SUBQUERY_ALIAS.VCOL FROM "
            + "(SELECT * FROM " + executor.delimitIdentifier(tableName)
            + " WHERE ${CONDITIONS}) SQOOP_SUBQUERY_ALIAS");

    GenericJdbcImportPartition partition;

    Extractor extractor = new GenericJdbcImportExtractor();
    DummyWriter writer = new DummyWriter();

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-50 <= ICOL AND ICOL < -16");
    extractor.run(context, partition, writer);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-16 <= ICOL AND ICOL < 17");
    extractor.run(context, partition, writer);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("17 <= ICOL AND ICOL < 50");
    extractor.run(context, partition, writer);
  }

  public class DummyContext implements MutableContext {
    HashMap<String, String> store = new HashMap<String, String>();

    @Override
    public String getString(String key) {
      return store.get(key);
    }

    @Override
    public void setString(String key, String value) {
      store.put(key, value);
    }
  }

  public class DummyWriter extends DataWriter {
    int indx = START;

    @Override
    public void writeArrayRecord(Object[] array) {
      for (int i = 0; i < array.length; i++) {
        if (array[i] instanceof Integer) {
          assertEquals(indx, ((Integer)array[i]).intValue());
        } else if (array[i] instanceof Double) {
          assertEquals((double)indx, ((Double)array[i]).doubleValue());
        } else {
          assertEquals(String.valueOf(indx), array[i].toString());
        }
      }
      indx++;
    }

    @Override
    public void writeCsvRecord(String csv) {
      fail("This method should not be invoked.");
    }

    @Override
    public void writeRecord(Object record) {
      fail("This method should not be invoked.");
    }
  }

}
