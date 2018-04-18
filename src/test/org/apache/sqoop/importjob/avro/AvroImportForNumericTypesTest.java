/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.importjob.avro;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.importjob.ImportJobTestConfiguration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.AvroTestUtils;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.adapter.DatabaseAdapter;
import org.apache.sqoop.testutil.adapter.MSSQLServerDatabaseAdapter;
import org.apache.sqoop.testutil.adapter.MySqlDatabaseAdapter;
import org.apache.sqoop.testutil.adapter.OracleDatabaseAdapter;
import org.apache.sqoop.testutil.adapter.PostgresDatabaseAdapter;
import org.apache.sqoop.importjob.avro.configuration.MSSQLServerImportJobTestConfiguration;
import org.apache.sqoop.importjob.avro.configuration.MySQLImportJobTestConfiguration;
import org.apache.sqoop.importjob.avro.configuration.OracleImportJobTestConfigurationForNumber;
import org.apache.sqoop.importjob.avro.configuration.OracleImportJobTestConfiguration;
import org.apache.sqoop.importjob.avro.configuration.PostgresqlImportJobTestConfigurationForNumeric;
import org.apache.sqoop.importjob.avro.configuration.PostgresqlImportJobTestConfigurationPaddingShouldSucceed;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
/**
 * This test covers the behavior of the Avro import for fixed point decimal types, i.e. NUMBER, NUMERIC
 * and DECIMAL.
 *
 * Oracle and Postgres store numbers without padding, while other DBs store them padded with 0s.
 *
 * The features tested here affect two phases in Sqoop:
 * 1. Avro schema generation
 * Default precision and scale are used here to avoid issues with Oracle and Postgres, as these
 * don't return valid precision and scale if they weren't specified in the table DDL.
 *
 * 2. Avro import: padding.
 * In case of Oracle and Postgres, Sqoop has to pad the values with 0s to avoid errors.
 */
public class AvroImportForNumericTypesTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      AvroImportForNumericTypesTest.class.getName());

  private Configuration conf = new Configuration();

  private final ImportJobTestConfiguration configuration;
  private final DatabaseAdapter adapter;
  private final boolean failWithoutExtraArgs;
  private final boolean failWithPadding;

  // Constants for the basic test case, that doesn't use extra arguments
  // that are required to avoid errors, i.e. padding and default precision and scale.
  private final static boolean SUCCEED_WITHOUT_EXTRA_ARGS = false;
  private final static boolean FAIL_WITHOUT_EXTRA_ARGS = true;

  // Constants for the test case that has padding specified but not default precision and scale.
  private final static boolean SUCCEED_WITH_PADDING_ONLY = false;
  private final static boolean FAIL_WITH_PADDING_ONLY = true;

  @Parameters(name = "Adapter: {0}| Config: {1}| failWithoutExtraArgs: {2}| failWithPadding: {3}")
  public static Iterable<? extends Object> testConfigurations() {
    DatabaseAdapter postgresAdapter = new PostgresDatabaseAdapter();
    OracleDatabaseAdapter oracleDatabaseAdapter = new OracleDatabaseAdapter();
    return Arrays.asList(
        new Object[] {oracleDatabaseAdapter, new OracleImportJobTestConfigurationForNumber(), FAIL_WITHOUT_EXTRA_ARGS, FAIL_WITH_PADDING_ONLY},
        new Object[] {oracleDatabaseAdapter, new OracleImportJobTestConfiguration(), FAIL_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY},
        new Object[] { new MySqlDatabaseAdapter(), new MySQLImportJobTestConfiguration(), SUCCEED_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY},
        new Object[] { new MSSQLServerDatabaseAdapter(), new MSSQLServerImportJobTestConfiguration(), SUCCEED_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY},
        new Object[] { postgresAdapter, new PostgresqlImportJobTestConfigurationForNumeric(), FAIL_WITHOUT_EXTRA_ARGS, FAIL_WITH_PADDING_ONLY},
        new Object[] { postgresAdapter, new PostgresqlImportJobTestConfigurationPaddingShouldSucceed(), SUCCEED_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY}
    );
  }

  public AvroImportForNumericTypesTest(DatabaseAdapter adapter, ImportJobTestConfiguration configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
    this.adapter = adapter;
    this.configuration = configuration;
    this.failWithoutExtraArgs = failWithoutExtraArgs;
    this.failWithPadding = failWithPaddingOnly;
  }

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
    return adapter.getConnectionString();
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    adapter.injectConnectionParameters(opts);
    return opts;
  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    adapter.dropTableIfExists(table, getManager());
  }

  @Before
  public void setUp() {
    super.setUp();
    String[] names = configuration.getNames();
    String[] types = configuration.getTypes();
    createTableWithColTypesAndNames(names, types, new String[0]);
    List<String[]> inputData = configuration.getSampleData();
    for (String[] input  : inputData) {
      insertIntoTable(names, types, input);
    }
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

  private ArgumentArrayBuilder getArgsBuilder() {
    ArgumentArrayBuilder builder = AvroTestUtils.getBuilderForAvroPaddingTest(this);
    builder.withOption("connect", getConnectString());
    return builder;
  }

  @Test
  public void testAvroImportWithoutPadding() throws IOException {
    if (failWithoutExtraArgs) {
      thrown.expect(IOException.class);
      thrown.expectMessage("Failure during job; return status 1");
    }
    String[] args = getArgsBuilder().build();
    runImport(args);
    if (!failWithoutExtraArgs) {
      verify();
    }
  }

  @Test
  public void testAvroImportWithPadding() throws IOException {
    if (failWithPadding) {
      thrown.expect(IOException.class);
      thrown.expectMessage("Failure during job; return status 1");
    }
    ArgumentArrayBuilder builder = getArgsBuilder();
    builder.withProperty("sqoop.avro.decimal_padding.enable", "true");
    runImport(builder.build());
    if (!failWithPadding) {
      verify();
    }
  }

  @Test
  public void testAvroImportWithDefaultPrecisionAndScale() throws  IOException {
    ArgumentArrayBuilder builder = getArgsBuilder();
    builder.withProperty("sqoop.avro.decimal_padding.enable", "true");
    builder.withProperty("sqoop.avro.logical_types.decimal.default.precision", "38");
    builder.withProperty("sqoop.avro.logical_types.decimal.default.scale", "3");
    runImport(builder.build());
    verify();
  }

  private void verify() {
    AvroTestUtils.verify(configuration.getExpectedResults(), getConf(), getTablePath());
  }
}
