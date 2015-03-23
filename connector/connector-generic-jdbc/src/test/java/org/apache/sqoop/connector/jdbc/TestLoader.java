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

import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.DateTime;

import org.apache.sqoop.schema.type.Time;

import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.Text;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.fail;

public class TestLoader {

  private final String tableName;

  private GenericJdbcExecutor executor;

  private static final int START = -50;

  private int numberOfRows;

  @DataProvider(name="test-jdbc-loader")
  public static Object[][] data() {
    return new Object[][] {{50}, {100}, {101}, {150}, {200}};
  }

  @Factory(dataProvider="test-jdbc-loader")
  public TestLoader(int numberOfRows) {
    this.numberOfRows = numberOfRows;
    this.tableName = getClass().getSimpleName().toUpperCase();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    executor = new GenericJdbcExecutor(GenericJdbcTestConstants.LINK_CONFIG);

    if (!executor.existTable(tableName)) {
      executor.executeUpdate("CREATE TABLE "
          + executor.delimitIdentifier(tableName)
          + "(ICOL INTEGER PRIMARY KEY, DCOL DOUBLE, VCOL VARCHAR(20), DATECOL DATE, DATETIMECOL TIMESTAMP, TIMECOL TIME)");
    } else {
      executor.deleteTableData(tableName);
    }
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    executor.close();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testInsert() throws Exception {
    MutableContext context = new MutableMapContext();

    LinkConfiguration linkConfig = new LinkConfiguration();

    linkConfig.linkConfig.jdbcDriver = GenericJdbcTestConstants.DRIVER;
    linkConfig.linkConfig.connectionString = GenericJdbcTestConstants.URL;

    ToJobConfiguration jobConfig = new ToJobConfiguration();

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_TO_DATA_SQL,
        "INSERT INTO " + executor.delimitIdentifier(tableName) + " VALUES (?,?,?,?,?,?)");


    Loader loader = new GenericJdbcLoader();
    DummyReader reader = new DummyReader();
    Schema schema = new Schema("TestLoader");
    schema.addColumn(new FixedPoint("c1", 2L, true)).addColumn(new Decimal("c2", 5, 2))
        .addColumn(new Text("c3")).addColumn(new Date("c4"))
        .addColumn(new DateTime("c5", false, false)).addColumn(new Time("c6", false));
    LoaderContext loaderContext = new LoaderContext(context, reader, schema);
    loader.load(loaderContext, linkConfig, jobConfig);

    int index = START;
    ResultSet rs = executor.executeQuery("SELECT * FROM "
        + executor.delimitIdentifier(tableName) + " ORDER BY ICOL");
    while (rs.next()) {
      assertEquals(index, rs.getObject(1));
      assertEquals((double) index, rs.getObject(2));
      assertEquals(String.valueOf(index), rs.getObject(3));
      assertEquals("2004-10-19", rs.getObject(4).toString());
      assertEquals("2004-10-19 10:23:34.0", rs.getObject(5).toString());
      assertEquals("11:33:59", rs.getObject(6).toString());

      index++;
    }
    assertEquals(numberOfRows, index-START);
  }

  public class DummyReader extends DataReader {
    int index = 0;

    @Override
    public Object[] readArrayRecord() {
      LocalDate jodaDate= new LocalDate(2004, 10, 19);
      org.joda.time.DateTime jodaDateTime= new org.joda.time.DateTime(2004, 10, 19, 10, 23, 34);
      LocalTime time= new LocalTime(11, 33, 59);

      if (index < numberOfRows) {
        Object[] array = new Object[] {
            START + index,
            (double) (START + index),
            String.valueOf(START+index),
            jodaDate,
            jodaDateTime,
            time};
        index++;
        return array;
      } else {
        return null;
      }
    }

    @Override
    public String readTextRecord() {
      fail("This method should not be invoked.");
      return null;
    }

    @Override
    public Object readContent() throws Exception {
      fail("This method should not be invoked.");
      return null;
    }

  }
}
