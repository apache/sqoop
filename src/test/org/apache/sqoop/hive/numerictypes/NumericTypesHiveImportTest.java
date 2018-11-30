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

package org.apache.sqoop.hive.numerictypes;

import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.NoAuthenticationConfiguration;
import org.apache.sqoop.importjob.configuration.HiveTestConfiguration;
import org.apache.sqoop.importjob.configuration.MysqlImportJobTestConfiguration;
import org.apache.sqoop.importjob.configuration.OracleImportJobTestConfiguration;
import org.apache.sqoop.importjob.configuration.OracleImportJobTestConfigurationForNumber;
import org.apache.sqoop.importjob.configuration.PostgresqlImportJobTestConfigurationForNumeric;
import org.apache.sqoop.importjob.configuration.PostgresqlImportJobTestConfigurationPaddingShouldSucceed;
import org.apache.sqoop.importjob.configuration.SqlServerImportJobTestConfiguration;
import org.apache.sqoop.testcategories.thirdpartytest.MysqlTest;
import org.apache.sqoop.testcategories.thirdpartytest.OracleTest;
import org.apache.sqoop.testcategories.thirdpartytest.PostgresqlTest;
import org.apache.sqoop.testcategories.thirdpartytest.SqlServerTest;
import org.apache.sqoop.testutil.HiveServer2TestUtil;
import org.apache.sqoop.testutil.NumericTypesTestUtils;
import org.apache.sqoop.testutil.adapter.DatabaseAdapter;
import org.apache.sqoop.testutil.adapter.MysqlDatabaseAdapter;
import org.apache.sqoop.testutil.adapter.OracleDatabaseAdapter;
import org.apache.sqoop.testutil.adapter.PostgresDatabaseAdapter;
import org.apache.sqoop.testutil.adapter.SqlServerDatabaseAdapter;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.sqoop.testutil.NumericTypesTestUtils.FAIL_WITHOUT_EXTRA_ARGS;
import static org.apache.sqoop.testutil.NumericTypesTestUtils.FAIL_WITH_PADDING_ONLY;
import static org.apache.sqoop.testutil.NumericTypesTestUtils.SUCCEED_WITHOUT_EXTRA_ARGS;
import static org.apache.sqoop.testutil.NumericTypesTestUtils.SUCCEED_WITH_PADDING_ONLY;

@RunWith(Enclosed.class)
public class NumericTypesHiveImportTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static HiveMiniCluster hiveMiniCluster;

  private static HiveServer2TestUtil hiveServer2TestUtil;

  @BeforeClass
  public static void beforeClass() {
    startHiveMiniCluster();
  }

  @AfterClass
  public static void afterClass() {
    stopHiveMiniCluster();
  }

  public static void startHiveMiniCluster() {
    hiveMiniCluster = new HiveMiniCluster(new NoAuthenticationConfiguration());
    hiveMiniCluster.start();
    hiveServer2TestUtil = new HiveServer2TestUtil(hiveMiniCluster.getUrl());
  }

  public static void stopHiveMiniCluster() {
    hiveMiniCluster.stop();
  }

  @Category(MysqlTest.class)
  public static class MysqlNumericTypesHiveImportTest extends NumericTypesHiveImportTestBase {

    public MysqlNumericTypesHiveImportTest() {
      super(new MysqlImportJobTestConfiguration(), NumericTypesTestUtils.SUCCEED_WITHOUT_EXTRA_ARGS, NumericTypesTestUtils.SUCCEED_WITH_PADDING_ONLY,
          hiveMiniCluster, hiveServer2TestUtil);
    }

    @Override
    public DatabaseAdapter createAdapter() {
      return new MysqlDatabaseAdapter();
    }
  }

  @Category(OracleTest.class)
  @RunWith(Parameterized.class)
  @Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
  public static class OracleNumericTypesHiveImportTest extends NumericTypesHiveImportTestBase {

    @Override
    public DatabaseAdapter createAdapter() {
      return new OracleDatabaseAdapter();
    }

    @Parameterized.Parameters(name = "Config: {0}| failWithoutExtraArgs: {1}| failWithPadding: {2}")
    public static Iterable<? extends Object> testConfigurations() {
      return Arrays.asList(
          new Object[]{new OracleImportJobTestConfigurationForNumber(), FAIL_WITHOUT_EXTRA_ARGS, FAIL_WITH_PADDING_ONLY},
          new Object[]{new OracleImportJobTestConfiguration(), FAIL_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY}
      );
    }

    public OracleNumericTypesHiveImportTest(HiveTestConfiguration configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
      super(configuration, failWithoutExtraArgs, failWithPaddingOnly, hiveMiniCluster, hiveServer2TestUtil);
    }
  }

  @Category(PostgresqlTest.class)
  @RunWith(Parameterized.class)
  @Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
  public static class PostgresNumericTypesHiveImportTest extends NumericTypesHiveImportTestBase {

    @Override
    public DatabaseAdapter createAdapter() {
      return new PostgresDatabaseAdapter();
    }

    @Parameterized.Parameters(name = "Config: {0}| failWithoutExtraArgs: {1}| failWithPadding: {2}")
    public static Iterable<? extends Object> testConfigurations() {
      return Arrays.asList(
          new Object[]{new PostgresqlImportJobTestConfigurationForNumeric(), FAIL_WITHOUT_EXTRA_ARGS, FAIL_WITH_PADDING_ONLY},
          new Object[]{new PostgresqlImportJobTestConfigurationPaddingShouldSucceed(), SUCCEED_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY}
      );
    }

    public PostgresNumericTypesHiveImportTest(HiveTestConfiguration configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
      super(configuration, failWithoutExtraArgs, failWithPaddingOnly, hiveMiniCluster, hiveServer2TestUtil);
    }
  }

  @Category(SqlServerTest.class)
  public static class SqlServerNumericTypesHiveImportTest extends NumericTypesHiveImportTestBase {

    public SqlServerNumericTypesHiveImportTest() {
      super(new SqlServerImportJobTestConfiguration(), SUCCEED_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY,
          hiveMiniCluster, hiveServer2TestUtil);
    }

    @Override
    public DatabaseAdapter createAdapter() {
      return new SqlServerDatabaseAdapter();
    }
  }

}
