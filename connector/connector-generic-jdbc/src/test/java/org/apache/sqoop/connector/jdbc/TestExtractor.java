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

import static org.testng.AssertJUnit.assertNull;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.Text;
import org.joda.time.LocalDate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.fail;


public class TestExtractor {

  private final String tableName;
  private final String nullDataTableName;

  private GenericJdbcExecutor executor;

  private static final int START = -50;
  private static final int NUMBER_OF_ROWS = 101;

  private static final double EPSILON = 0.01;

  public TestExtractor() {
    tableName = getClass().getSimpleName().toUpperCase();
   nullDataTableName = getClass().getSimpleName().toUpperCase() + "NULL";
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.LINK_CONFIG);

    if (!executor.existTable(tableName)) {
      executor.executeUpdate("CREATE TABLE "
          + executor.delimitIdentifier(tableName)
          + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20), DATECOL DATE)");

      for (int i = 0; i < NUMBER_OF_ROWS; i++) {
        int value = START + i;
        String sql = "INSERT INTO " + executor.delimitIdentifier(tableName)
            + " VALUES(" + value + ", " + value + ", '" + value + "', '2004-10-19')";
        executor.executeUpdate(sql);
      }
    }
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    executor.close();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testQuery() throws Exception {
    MutableContext context = new MutableMapContext();

    LinkConfiguration linkConfig = new LinkConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;

    FromJobConfiguration jobConfig = new FromJobConfiguration();

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL,
        "SELECT * FROM " + executor.delimitIdentifier(tableName) + " WHERE ${CONDITIONS}");

    GenericJdbcPartition partition;

    Extractor extractor = new GenericJdbcExtractor();
    DummyWriter writer = new DummyWriter();
    Schema schema = new Schema("TestExtractor");
    // dummy columns added, all we need is the column count to match to the
    // result set
    schema.addColumn(new FixedPoint("c1",2L, true)).addColumn(new Decimal("c2", 5, 2)).addColumn(new Text("c3")).addColumn(new Date("c4"));

    ExtractorContext extractorContext = new ExtractorContext(context, writer, schema);

    partition = new GenericJdbcPartition();
    partition.setConditions("-50.0 <= DCOL AND DCOL < -16.6666666666666665");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);

    partition = new GenericJdbcPartition();
    partition.setConditions("-16.6666666666666665 <= DCOL AND DCOL < 16.666666666666667");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);

    partition = new GenericJdbcPartition();
    partition.setConditions("16.666666666666667 <= DCOL AND DCOL <= 50.0");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testSubquery() throws Exception {
    MutableContext context = new MutableMapContext();

    LinkConfiguration linkConfig = new LinkConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;

    FromJobConfiguration jobConfig = new FromJobConfiguration();

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL,
        "SELECT SQOOP_SUBQUERY_ALIAS.ICOL,SQOOP_SUBQUERY_ALIAS.VCOL,SQOOP_SUBQUERY_ALIAS.DATECOL FROM " + "(SELECT * FROM "
            + executor.delimitIdentifier(tableName) + " WHERE ${CONDITIONS}) SQOOP_SUBQUERY_ALIAS");

    GenericJdbcPartition partition;

    Extractor extractor = new GenericJdbcExtractor();
    DummyWriter writer = new DummyWriter();
    Schema schema = new Schema("TestExtractor");
    // dummy columns added, all we need is the column count to match to the
    // result set
    schema.addColumn(new FixedPoint("c1", 2L, true)).addColumn(new Text("c2")).addColumn(new Date("c3"));

    ExtractorContext extractorContext = new ExtractorContext(context, writer, schema);

    partition = new GenericJdbcPartition();
    partition.setConditions("-50 <= ICOL AND ICOL < -16");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);

    partition = new GenericJdbcPartition();
    partition.setConditions("-16 <= ICOL AND ICOL < 17");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);

    partition = new GenericJdbcPartition();
    partition.setConditions("17 <= ICOL AND ICOL < 50");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test(expectedExceptions = SqoopException.class)
  public void testIncorrectSchemaColumnSize() throws Exception {
    MutableContext context = new MutableMapContext();

    LinkConfiguration linkConfig = new LinkConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;

    FromJobConfiguration jobConfig = new FromJobConfiguration();

    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL,
        "SELECT SQOOP_SUBQUERY_ALIAS.ICOL,SQOOP_SUBQUERY_ALIAS.VCOL FROM " + "(SELECT * FROM "
            + executor.delimitIdentifier(tableName) + " WHERE ${CONDITIONS}) SQOOP_SUBQUERY_ALIAS");

    GenericJdbcPartition partition = new GenericJdbcPartition();

    Extractor extractor = new GenericJdbcExtractor();
    DummyWriter writer = new DummyWriter();
    Schema schema = new Schema("TestIncorrectColumns");
    ExtractorContext extractorContext = new ExtractorContext(context, writer, schema);

    partition.setConditions("-50 <= ICOL AND ICOL < -16");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);

  }

  @Test
  public void testNullValueExtracted() throws Exception {

    if (!executor.existTable(nullDataTableName)) {
      executor.executeUpdate("CREATE TABLE " + executor.delimitIdentifier(nullDataTableName)
          + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20), DATECOL DATE)");

      for (int i = 0; i < NUMBER_OF_ROWS; i++) {
        int value = i;
        String sql = "INSERT INTO " + executor.delimitIdentifier(nullDataTableName) + " VALUES(" + value + ",null,null,null)";
        executor.executeUpdate(sql);
      }
    }
    MutableContext context = new MutableMapContext();

    LinkConfiguration linkConfig = new LinkConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;

    FromJobConfiguration jobConfig = new FromJobConfiguration();
    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL,
        "SELECT * FROM " + executor.delimitIdentifier(nullDataTableName) + " WHERE ${CONDITIONS}");

    Extractor extractor = new GenericJdbcExtractor();
    DummyNullDataWriter writer = new DummyNullDataWriter();
    Schema schema = new Schema("TestExtractor");
    schema.addColumn(new FixedPoint("c1",2L, true)).addColumn(new Decimal("c2", 5, 2)).addColumn(new Text("c3")).addColumn(new Date("c4"));

    ExtractorContext extractorContext = new ExtractorContext(context, writer, schema);

    GenericJdbcPartition partition = new GenericJdbcPartition();
    partition.setConditions("-50 <= ICOL AND ICOL < -16");
    extractor.extract(extractorContext, linkConfig, jobConfig, partition);

  }

  public class DummyWriter extends DataWriter {
    int indx = START;

    @Override
    public void writeArrayRecord(Object[] array) {
      boolean parsedDate = false;
      for (int i = 0; i < array.length; i++) {
        if (array[i] instanceof Integer) {
          assertEquals(indx, ((Integer) array[i]).intValue());
        } else if (array[i] instanceof Double) {
          assertEquals((double)indx, ((Double)array[i]).doubleValue(), EPSILON);
        } else if (array[i] instanceof String) {
          assertEquals(String.valueOf(indx), array[i].toString());
        } else if (array[i] instanceof LocalDate) {
          assertEquals("2004-10-19", array[i].toString());
          parsedDate = true;
        }
      }
      indx++;
      assertEquals(true, parsedDate);
    }

    @Override
    public void writeStringRecord(String text) {
      fail("This method should not be invoked.");
    }

    @Override
    public void writeRecord(Object content) {
      fail("This method should not be invoked.");
    }
  }

  public class DummyNullDataWriter extends DataWriter {

    @Override
    public void writeArrayRecord(Object[] array) {
      for (int i = 0; i < array.length; i++) {
        // primary key cant be null
        if (i > 0) {
          assertNull(array[i]);
        }
      }
    }

    @Override
    public void writeStringRecord(String text) {
      fail("This method should not be invoked.");
    }

    @Override
    public void writeRecord(Object content) {
      fail("This method should not be invoked.");
    }
  }
}