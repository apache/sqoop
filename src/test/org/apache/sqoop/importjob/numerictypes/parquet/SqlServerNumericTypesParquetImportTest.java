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

package org.apache.sqoop.importjob.numerictypes.parquet;

import org.apache.sqoop.importjob.configuration.SqlServerImportJobTestConfiguration;
import org.apache.sqoop.importjob.numerictypes.NumericTypesParquetImportTestBase;
import org.apache.sqoop.testcategories.thirdpartytest.SqlServerTest;
import org.apache.sqoop.testutil.adapter.DatabaseAdapter;
import org.apache.sqoop.testutil.adapter.SqlServerDatabaseAdapter;
import org.junit.experimental.categories.Category;

import static org.apache.sqoop.testutil.NumericTypesTestUtils.SUCCEED_WITHOUT_EXTRA_ARGS;
import static org.apache.sqoop.testutil.NumericTypesTestUtils.SUCCEED_WITH_PADDING_ONLY;

@Category(SqlServerTest.class)
public class SqlServerNumericTypesParquetImportTest extends NumericTypesParquetImportTestBase {

  @Override
  public DatabaseAdapter createAdapter() {
    return new SqlServerDatabaseAdapter();
  }

  public SqlServerNumericTypesParquetImportTest() {
    super(new SqlServerImportJobTestConfiguration(), SUCCEED_WITHOUT_EXTRA_ARGS, SUCCEED_WITH_PADDING_ONLY);
  }
}
