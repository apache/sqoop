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

package org.apache.sqoop.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorImplementation;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import static org.apache.sqoop.mapreduce.parquet.ParquetJobConfiguratorImplementation.HADOOP;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class TestBaseSqoopTool {

  private static final String PARQUET_CONFIGURATOR_IMPLEMENTATION = "parquet-configurator-implementation";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private BaseSqoopTool testBaseSqoopTool;
  private SqoopOptions testSqoopOptions;
  private CommandLine mockCommandLine;

  @Before
  public void setup() {
    testBaseSqoopTool = mock(BaseSqoopTool.class, Mockito.CALLS_REAL_METHODS);
    testSqoopOptions = new SqoopOptions();
    mockCommandLine = mock(CommandLine.class);
  }

  @Test
  public void testRethrowIfRequiredWithoutRethrowPropertySetOrThrowOnErrorOption() {
    testSqoopOptions.setThrowOnError(false);

    testBaseSqoopTool.rethrowIfRequired(testSqoopOptions, new Exception());
  }

  @Test
  public void testRethrowIfRequiredWithRethrowPropertySetAndRuntimeException() {
    RuntimeException expectedException = new RuntimeException();
    testSqoopOptions.setThrowOnError(true);


    exception.expect(sameInstance(expectedException));
    testBaseSqoopTool.rethrowIfRequired(testSqoopOptions, expectedException);
  }

  @Test
  public void testRethrowIfRequiredWithRethrowPropertySetAndException() {
    Exception expectedCauseException = new Exception();
    testSqoopOptions.setThrowOnError(true);

    exception.expect(RuntimeException.class);
    exception.expectCause(sameInstance(expectedCauseException));
    testBaseSqoopTool.rethrowIfRequired(testSqoopOptions, expectedCauseException);
  }

  @Test
  public void testApplyCommonOptionsSetsParquetJobConfigurationImplementationFromCommandLine() throws Exception {
    ParquetJobConfiguratorImplementation expectedValue = HADOOP;

    when(mockCommandLine.getOptionValue(PARQUET_CONFIGURATOR_IMPLEMENTATION)).thenReturn(expectedValue.toString());

    testBaseSqoopTool.applyCommonOptions(mockCommandLine, testSqoopOptions);

    assertEquals(expectedValue, testSqoopOptions.getParquetConfiguratorImplementation());
  }

  @Test
  public void testApplyCommonOptionsSetsParquetJobConfigurationImplementationFromCommandLineCaseInsensitively() throws Exception {
    String hadoopImplementationLowercase = "haDooP";

    when(mockCommandLine.getOptionValue(PARQUET_CONFIGURATOR_IMPLEMENTATION)).thenReturn(hadoopImplementationLowercase);

    testBaseSqoopTool.applyCommonOptions(mockCommandLine, testSqoopOptions);

    assertEquals(HADOOP, testSqoopOptions.getParquetConfiguratorImplementation());
  }

  @Test
  public void testApplyCommonOptionsSetsParquetJobConfigurationImplementationFromConfiguration() throws Exception {
    ParquetJobConfiguratorImplementation expectedValue = HADOOP;
    testSqoopOptions.getConf().set("parquetjob.configurator.implementation", expectedValue.toString());

    testBaseSqoopTool.applyCommonOptions(mockCommandLine, testSqoopOptions);

    assertEquals(expectedValue, testSqoopOptions.getParquetConfiguratorImplementation());
  }

  @Test
  public void testApplyCommonOptionsPrefersParquetJobConfigurationImplementationFromCommandLine() throws Exception {
    ParquetJobConfiguratorImplementation expectedValue = HADOOP;
    testSqoopOptions.getConf().set("parquetjob.configurator.implementation", "kite");
    when(mockCommandLine.getOptionValue(PARQUET_CONFIGURATOR_IMPLEMENTATION)).thenReturn(expectedValue.toString());

    testBaseSqoopTool.applyCommonOptions(mockCommandLine, testSqoopOptions);

    assertEquals(expectedValue, testSqoopOptions.getParquetConfiguratorImplementation());
  }

  @Test
  public void testApplyCommonOptionsThrowsWhenInvalidParquetJobConfigurationImplementationIsSet() throws Exception {
    when(mockCommandLine.getOptionValue(PARQUET_CONFIGURATOR_IMPLEMENTATION)).thenReturn("this_is_definitely_not_valid");

    exception.expectMessage("Invalid Parquet job configurator implementation is set: this_is_definitely_not_valid. Supported values are: [HADOOP]");
    testBaseSqoopTool.applyCommonOptions(mockCommandLine, testSqoopOptions);
  }

  @Test
  public void testApplyCommonOptionsDoesNotChangeDefaultParquetJobConfigurationImplementationWhenNothingIsSet() throws Exception {
    testBaseSqoopTool.applyCommonOptions(mockCommandLine, testSqoopOptions);

    assertEquals(HADOOP, testSqoopOptions.getParquetConfiguratorImplementation());
  }

  @Test
  public void testGetCommonOptionsAddsParquetJobConfigurationImplementation() {
    RelatedOptions commonOptions = testBaseSqoopTool.getCommonOptions();

    assertTrue(commonOptions.hasOption(PARQUET_CONFIGURATOR_IMPLEMENTATION));
  }

  @Test
  public void testParquetJobConfigurationImplementationOptionHasAnArg() {
    RelatedOptions commonOptions = testBaseSqoopTool.getCommonOptions();

    Option implementationOption = commonOptions.getOption(PARQUET_CONFIGURATOR_IMPLEMENTATION);
    assertTrue(implementationOption.hasArg());
  }
}
