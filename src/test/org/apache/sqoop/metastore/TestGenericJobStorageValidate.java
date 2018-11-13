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

package org.apache.sqoop.metastore;

import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;

@RunWith(Parameterized.class)
@Category(UnitTest.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestGenericJobStorageValidate {

  @Parameters(name = "metastoreConnectionString = {0}, validationShouldFail = {1}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(
        new Object[]{"jdbc:mysql://localhost/", false},
        new Object[]{"jdbc:oracle://localhost/", false},
        new Object[]{"jdbc:hsqldb://localhost/", false},
        new Object[]{"jdbc:postgresql://localhost/", false},
        new Object[]{"jdbc:sqlserver://localhost/", false},
        new Object[]{"jdbc:db2://localhost/", false},
        new Object[]{"jdbc:dummy://localhost/", true},
        new Object[]{null, true},
        new Object[]{"", true});
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final String connectionString;

  private final boolean expectedToFail;

  private GenericJobStorage jobStorage;

  public TestGenericJobStorageValidate(String connectionString, boolean expectedToFail) {
    this.connectionString = connectionString;
    this.expectedToFail = expectedToFail;
  }

  @Before
  public void before() {
    jobStorage = new GenericJobStorage();
  }

  @Test
  public void testValidateMetastoreConnectionStringWithParameters() {
    if (expectedToFail) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(connectionString + " is an invalid connection string or the required RDBMS is not supported.");
    }
    jobStorage.validateMetastoreConnectionString(connectionString);
  }

}
