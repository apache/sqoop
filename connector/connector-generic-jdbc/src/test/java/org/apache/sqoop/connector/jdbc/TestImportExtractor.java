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
import org.apache.sqoop.connector.jdbc.configuration.ImportJobConfiguration;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.etl.io.DataWriter;

public class TestImportExtractor extends TestCase {

  private final String tableName;

  private GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 101;

  public TestImportExtractor() {
    tableName = getClass().getSimpleName().toUpperCase();
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

  public void testQuery() throws Exception {
    MutableContext context = new MutableMapContext();

    ConnectionConfiguration connectionConfig = new ConnectionConfiguration();

    connectionConfig.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connectionConfig.connection.connectionString = GenericJdbcTestConstants.URL;

    ImportJobConfiguration jobConfig = new ImportJobConfiguration();

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
        "SELECT * FROM " + executor.delimitIdentifier(tableName) + " WHERE ${CONDITIONS}");

    GenericJdbcImportPartition partition;

    Extractor extractor = new GenericJdbcImportExtractor();
    DummyWriter writer = new DummyWriter();
    ExtractorContext extractorContext = new ExtractorContext(context, writer);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-50.0 <= DCOL AND DCOL < -16.6666666666666665");
    extractor.extract(extractorContext, connectionConfig, jobConfig, partition);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-16.6666666666666665 <= DCOL AND DCOL < 16.666666666666667");
    extractor.extract(extractorContext, connectionConfig, jobConfig, partition);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("16.666666666666667 <= DCOL AND DCOL <= 50.0");
    extractor.extract(extractorContext, connectionConfig, jobConfig, partition);
  }

  public void testSubquery() throws Exception {
    MutableContext context = new MutableMapContext();

    ConnectionConfiguration connectionConfig = new ConnectionConfiguration();

    connectionConfig.connection.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    connectionConfig.connection.connectionString = GenericJdbcTestConstants.URL;

    ImportJobConfiguration jobConfig = new ImportJobConfiguration();

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
        "SELECT SQOOP_SUBQUERY_ALIAS.ICOL,SQOOP_SUBQUERY_ALIAS.VCOL FROM "
            + "(SELECT * FROM " + executor.delimitIdentifier(tableName)
            + " WHERE ${CONDITIONS}) SQOOP_SUBQUERY_ALIAS");

    GenericJdbcImportPartition partition;

    Extractor extractor = new GenericJdbcImportExtractor();
    DummyWriter writer = new DummyWriter();
    ExtractorContext extractorContext = new ExtractorContext(context, writer);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-50 <= ICOL AND ICOL < -16");
    extractor.extract(extractorContext, connectionConfig, jobConfig, partition);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("-16 <= ICOL AND ICOL < 17");
    extractor.extract(extractorContext, connectionConfig, jobConfig, partition);

    partition = new GenericJdbcImportPartition();
    partition.setConditions("17 <= ICOL AND ICOL < 50");
    extractor.extract(extractorContext, connectionConfig, jobConfig, partition);
  }

  public class DummyWriter extends DataWriter {
    int indx = START;

    @Override
    public void setFieldDelimiter(char fieldDelimiter) {
      // do nothing and use default delimiter
    }

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
    public void writeContent(Object content, int type) {
      fail("This method should not be invoked.");
    }
  }
}
