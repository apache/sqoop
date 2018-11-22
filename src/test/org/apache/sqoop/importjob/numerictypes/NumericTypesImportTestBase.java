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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.importjob.DatabaseAdapterFactory;
import org.apache.sqoop.importjob.configuration.AvroTestConfiguration;
import org.apache.sqoop.importjob.configuration.ParquetTestConfiguration;
import org.apache.sqoop.testcategories.thirdpartytest.ThirdPartyTest;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.AvroTestUtils;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.adapter.DatabaseAdapter;
import org.apache.sqoop.util.ParquetReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.apache.sqoop.SqoopOptions.FileLayout.AvroDataFile;
import static org.apache.sqoop.SqoopOptions.FileLayout.ParquetFile;
import static org.junit.Assert.assertEquals;

@Category(ThirdPartyTest.class)
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
public abstract class NumericTypesImportTestBase<T extends AvroTestConfiguration & ParquetTestConfiguration> extends ImportJobTestCase implements DatabaseAdapterFactory {

  public static final Log LOG = LogFactory.getLog(NumericTypesImportTestBase.class.getName());

  private Configuration conf = new Configuration();

  private final T configuration;
  private final DatabaseAdapter adapter;
  private final boolean failWithoutExtraArgs;
  private final boolean failWithPadding;

  // Constants for the basic test case, that doesn't use extra arguments
  // that are required to avoid errors, i.e. padding and default precision and scale.
  protected final static boolean SUCCEED_WITHOUT_EXTRA_ARGS = false;
  protected final static boolean FAIL_WITHOUT_EXTRA_ARGS = true;

  // Constants for the test case that has padding specified but not default precision and scale.
  protected final static boolean SUCCEED_WITH_PADDING_ONLY = false;
  protected final static boolean FAIL_WITH_PADDING_ONLY = true;

  private Path tableDirPath;

  public NumericTypesImportTestBase(T configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
    this.adapter = createAdapter();
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
    tableDirPath = new Path(getWarehouseDir() + "/" + getTableName());
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

  private ArgumentArrayBuilder getArgsBuilder(SqoopOptions.FileLayout fileLayout) {
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    if (AvroDataFile.equals(fileLayout)) {
      builder.withOption("as-avrodatafile");
    }
    else if (ParquetFile.equals(fileLayout)) {
      builder.withOption("as-parquetfile");
    }

    return builder.withCommonHadoopFlags(true)
        .withOption("warehouse-dir", getWarehouseDir())
        .withOption("num-mappers", "1")
        .withOption("table", getTableName())
        .withOption("connect", getConnectString());
  }

  /**
   * Adds properties to the given arg builder for decimal precision and scale.
   * @param builder
   */
  private void addPrecisionAndScale(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.avro.logical_types.decimal.default.precision", "38");
    builder.withProperty("sqoop.avro.logical_types.decimal.default.scale", "3");
  }

  /**
   * Enables padding for decimals in avro and parquet import.
   * @param builder
   */
  private void addPadding(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.avro.decimal_padding.enable", "true");
  }

  private void addEnableAvroDecimal(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.avro.logical_types.decimal.enable", "true");
  }

  private void addEnableParquetDecimal(ArgumentArrayBuilder builder) {
    builder.withProperty("sqoop.parquet.logical_types.decimal.enable", "true");
  }

  private void configureJunitToExpectFailure(boolean failWithPadding) {
    if (failWithPadding) {
      thrown.expect(IOException.class);
      thrown.expectMessage("Failure during job; return status 1");
    }
  }

  @Test
  public void testAvroImportWithoutPadding() throws IOException {
    configureJunitToExpectFailure(failWithoutExtraArgs);
    ArgumentArrayBuilder builder = getArgsBuilder(AvroDataFile);
    addEnableAvroDecimal(builder);
    String[] args = builder.build();
    runImport(args);
    if (!failWithoutExtraArgs) {
      verify(AvroDataFile);
    }
  }

  @Test
  public void testAvroImportWithPadding() throws IOException {
    configureJunitToExpectFailure(failWithPadding);
    ArgumentArrayBuilder builder = getArgsBuilder(AvroDataFile);
    addEnableAvroDecimal(builder);
    addPadding(builder);
    runImport(builder.build());
    if (!failWithPadding) {
      verify(AvroDataFile);
    }
  }

  @Test
  public void testAvroImportWithDefaultPrecisionAndScale() throws  IOException {
    ArgumentArrayBuilder builder = getArgsBuilder(AvroDataFile);
    addEnableAvroDecimal(builder);
    addPadding(builder);
    addPrecisionAndScale(builder);
    runImport(builder.build());
    verify(AvroDataFile);
  }

  @Test
  public void testParquetImportWithoutPadding() throws IOException {
    configureJunitToExpectFailure(failWithoutExtraArgs);
    ArgumentArrayBuilder builder = getArgsBuilder(ParquetFile);
    addEnableParquetDecimal(builder);
    String[] args = builder.build();
    runImport(args);
    if (!failWithoutExtraArgs) {
      verify(ParquetFile);
    }
  }

  @Test
  public void testParquetImportWithPadding() throws IOException {
    configureJunitToExpectFailure(failWithPadding);
    ArgumentArrayBuilder builder = getArgsBuilder(ParquetFile);
    addEnableParquetDecimal(builder);
    addPadding(builder);
    runImport(builder.build());
    if (!failWithPadding) {
      verify(ParquetFile);
    }
  }

  @Test
  public void testParquetImportWithDefaultPrecisionAndScale() throws IOException {
    ArgumentArrayBuilder builder = getArgsBuilder(ParquetFile);
    addEnableParquetDecimal(builder);
    addPadding(builder);
    addPrecisionAndScale(builder);
    runImport(builder.build());
    verify(ParquetFile);
  }

  private void verify(SqoopOptions.FileLayout fileLayout) {
    if (AvroDataFile.equals(fileLayout)) {
      AvroTestUtils.registerDecimalConversionUsageForVerification();
      AvroTestUtils.verify(configuration.getExpectedResultsForAvro(), getConf(), getTablePath());
    } else if (ParquetFile.equals(fileLayout)) {
      verifyParquetFile();
    }
  }

  private void verifyParquetFile() {
    verifyParquetSchema();
    verifyParquetContent();
  }

  private void verifyParquetContent() {
    ParquetReader reader = new ParquetReader(tableDirPath);
    assertEquals(Arrays.asList(configuration.getExpectedResultsForParquet()), reader.readAllInCsvSorted());
  }

  private void verifyParquetSchema() {
    ParquetReader reader = new ParquetReader(tableDirPath);
    MessageType parquetSchema = reader.readParquetSchema();

    String[] types = configuration.getTypes();
    for (int i = 0; i < types.length; i ++) {
      String type = types[i];
      if (isNumericSqlType(type)) {
        OriginalType parquetFieldType = parquetSchema.getFields().get(i).getOriginalType();
        assertEquals(OriginalType.DECIMAL, parquetFieldType);
      }
    }
  }

  private boolean isNumericSqlType(String type) {
    return type.toUpperCase().startsWith("DECIMAL")
        || type.toUpperCase().startsWith("NUMBER")
        || type.toUpperCase().startsWith("NUMERIC");
  }
}
