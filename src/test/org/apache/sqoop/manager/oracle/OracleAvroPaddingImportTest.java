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


package org.apache.sqoop.manager.oracle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.oracle.util.OracleUtils;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.AvroTestUtils;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

  public class OracleAvroPaddingImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      OracleAvroPaddingImportTest.class.getName());

  private  Configuration conf = new Configuration();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Override
  protected Configuration getConf() {
    return conf;
  }

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String getConnectString() {
    return org.apache.sqoop.manager.oracle.util.OracleUtils.CONNECT_STRING;
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    org.apache.sqoop.manager.oracle.util.OracleUtils.setOracleAuth(opts);
    return opts;
  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    OracleUtils.dropTable(table, getManager());
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
    builder.withOption("connect", getConnectString());
    return builder;
  }

  @Test
  public void testAvroImportWithoutPaddingFails() throws IOException {
    thrown.expect(IOException.class);
    thrown.expectMessage("Failure during job; return status 1");
    String[] args = getArgsBuilder().build();
    runImport(args);
  }

  @Test
  public void testAvroImportWithPadding() throws IOException {
    ArgumentArrayBuilder builder = getArgsBuilder();
    builder.withProperty("sqoop.avro.decimal_padding.enable", "true");
    runImport(builder.build());
    AvroTestUtils.verify(AvroTestUtils.getExpectedResults(), getConf(), getTablePath());
  }
}
