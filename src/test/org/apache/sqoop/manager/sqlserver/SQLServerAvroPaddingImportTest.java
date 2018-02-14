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

package org.apache.sqoop.manager.sqlserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.AvroTestUtils;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class SQLServerAvroPaddingImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
          SQLServerAvroPaddingImportTest.class.getName());

  private  Configuration conf = new Configuration();

  @Override
  protected String getConnectString() {
    return MSSQLTestUtils.CONNECT_STRING;
  }

  @Override
  protected Configuration getConf() {
    return conf;
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = new SqoopOptions();
    options.setConnectString(MSSQLTestUtils.CONNECT_STRING);
    options.setUsername(MSSQLTestUtils.DATABASE_USER);
    options.setPassword(MSSQLTestUtils.DATABASE_PASSWORD);
    return  options;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String dropTableIfExistsCommand(String table) {
    return "DROP TABLE IF EXISTS " + manager.escapeTableName(table);
  }

  @Before
  public void setUp() {
    super.setUp();
    String [] names = {"ID",  "NAME", "SALARY", "DEPT"};
    String [] types = { "INT", "VARCHAR(24)", "DECIMAL(20,5)", "VARCHAR(32)"};
    List<String[]> inputData = AvroTestUtils.getInputData();
    createTableWithColTypesAndNames(names, types, new String[0]);
    insertIntoTable(names, types, inputData.get(0));
    insertIntoTable(names, types, inputData.get(1));
    insertIntoTable(names, types, inputData.get(2));
  }

  @After
  public void tearDown() {
    try {
      dropTableIfExists(getTableName());
    } catch (SQLException e) {
      LOG.warn("Error trying to drop table on tearDown: " + e);
    }
    super.tearDown();
  }

  protected ArgumentArrayBuilder getArgsBuilder() {
    ArgumentArrayBuilder builder = AvroTestUtils.getBuilderForAvroPaddingTest(this);
    builder.withOption("connect", MSSQLTestUtils.CONNECT_STRING);
    builder.withOption("username", MSSQLTestUtils.DATABASE_USER);
    builder.withOption("password", MSSQLTestUtils.DATABASE_PASSWORD);
    return builder;
  }

  /**
   * Test for avro import with a number value in the table.
   * SQL Server stores the values padded in the database, therefore this import should always be successful
   * (Oracle for instance doesn't pad numbers in the database, therefore that one fails without the
   * sqoop.avro.decimal_padding.enable property)
   * @throws IOException
   */
  @Test
  public void testAvroImportWithoutPaddingFails() throws IOException {
    String[] args = getArgsBuilder().build();
    runImport(args);
    String [] expectedResults = AvroTestUtils.getExpectedResults();
    AvroTestUtils.verify(expectedResults, getConf(), getTablePath());
  }

  /**
   * This test covers a different code path than {@link #testAvroImportWithoutPaddingFails()},
   * since the BigDecimal values are checked and padded by Sqoop in
   * {@link AvroUtil#padBigDecimal(java.math.BigDecimal, org.apache.avro.Schema)}
   * No actual padding occurs, as the values coming back from SQL Server are already padded with 0s.
   * @throws IOException
   */
  @Test
  public void testAvroImportWithPadding() throws IOException {
    ArgumentArrayBuilder builder = getArgsBuilder();
    builder.withProperty("sqoop.avro.decimal_padding.enable", "true");
    runImport(builder.build());
    String [] expectedResults = AvroTestUtils.getExpectedResults();
    AvroTestUtils.verify(expectedResults, getConf(), getTablePath());
  }

}
