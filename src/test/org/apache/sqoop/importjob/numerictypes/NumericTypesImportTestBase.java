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

package org.apache.sqoop.importjob.numerictypes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.importjob.configuration.ImportJobTestConfiguration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.NumericTypesTestUtils;
import org.apache.sqoop.testutil.ThirdPartyTestBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * This test covers the behavior of the Avro import for fixed point decimal types, i.e. NUMBER, NUMERIC
 * and DECIMAL.
 *
 * Oracle and Postgres store numbers without padding, while other DBs store them padded with 0s.
 *
 * The features tested here affect two phases in Sqoop:
 * 1. Avro schema generation during avro and parquet import
 * Default precision and scale are used here to avoid issues with Oracle and Postgres, as these
 * don't return valid precision and scale if they weren't specified in the table DDL.
 *
 * 2. Decimal padding during avro or parquet import
 * In case of Oracle and Postgres, Sqoop has to pad the values with 0s to avoid errors.
 */
public abstract class NumericTypesImportTestBase<T extends ImportJobTestConfiguration> extends ThirdPartyTestBase<T>  {

  public static final Log LOG = LogFactory.getLog(NumericTypesImportTestBase.class.getName());

  private final boolean failWithoutExtraArgs;
  private final boolean failWithPadding;

  public NumericTypesImportTestBase(T configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
    super(configuration);
    this.failWithoutExtraArgs = failWithoutExtraArgs;
    this.failWithPadding = failWithPaddingOnly;
  }

  @Before
  public void setUp() {
    super.setUp();
    tableDirPath = new Path(getWarehouseDir() + "/" + getTableName());
  }

  protected Path tableDirPath;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  abstract public ArgumentArrayBuilder getArgsBuilder();
  abstract public void verify();

  public ArgumentArrayBuilder includeCommonOptions(ArgumentArrayBuilder builder) {
    return builder.withCommonHadoopFlags(true)
        .withOption("warehouse-dir", getWarehouseDir())
        .withOption("num-mappers", "1")
        .withOption("table", getTableName())
        .withOption("connect", getConnectString());
  }


  @Test
  public void testImportWithoutPadding() throws IOException {
    if(failWithoutExtraArgs){
      NumericTypesTestUtils.configureJunitToExpectFailure(thrown);
    }
    ArgumentArrayBuilder builder = getArgsBuilder();
    String[] args = builder.build();
    runImport(args);
    if (!failWithoutExtraArgs) {
      verify();
    }
  }

  @Test
  public void testImportWithPadding() throws IOException {
    if(failWithPadding){
      NumericTypesTestUtils.configureJunitToExpectFailure(thrown);
    }
    ArgumentArrayBuilder builder = getArgsBuilder();
    NumericTypesTestUtils.addPadding(builder);
    runImport(builder.build());
    if (!failWithPadding) {
      verify();
    }
  }

  @Test
  public void testImportWithDefaultPrecisionAndScale() throws IOException {
    ArgumentArrayBuilder builder = getArgsBuilder();
    NumericTypesTestUtils.addPadding(builder);
    NumericTypesTestUtils.addPrecisionAndScale(builder);
    runImport(builder.build());
    verify();
  }

}
