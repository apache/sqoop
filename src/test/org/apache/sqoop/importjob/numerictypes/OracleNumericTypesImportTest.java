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

package org.apache.sqoop.importjob.numerictypes;

import org.apache.sqoop.importjob.configuration.AvroTestConfiguration;
import org.apache.sqoop.importjob.configuration.OracleImportJobTestConfiguration;
import org.apache.sqoop.importjob.configuration.OracleImportJobTestConfigurationForNumber;
import org.apache.sqoop.importjob.configuration.ParquetTestConfiguration;
import org.apache.sqoop.testcategories.thirdpartytest.OracleTest;
import org.apache.sqoop.testutil.adapter.DatabaseAdapter;
import org.apache.sqoop.testutil.adapter.OracleDatabaseAdapter;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@Category(OracleTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class OracleNumericTypesImportTest<T extends AvroTestConfiguration & ParquetTestConfiguration> extends NumericTypesImportTestBase<T> {

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

  public OracleNumericTypesImportTest(T configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
    super(configuration, failWithoutExtraArgs, failWithPaddingOnly);
  }
}
