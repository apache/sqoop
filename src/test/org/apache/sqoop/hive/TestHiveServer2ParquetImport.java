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

package org.apache.sqoop.hive;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.NoAuthenticationConfiguration;
import org.apache.sqoop.testcategories.sqooptest.IntegrationTest;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.HiveServer2TestUtil;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.apache.sqoop.util.ParquetReader;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.deepEquals;
import static org.apache.sqoop.testutil.BaseSqoopTestCase.timeFromString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Enclosed.class)
@Category(IntegrationTest.class)
public class TestHiveServer2ParquetImport {

  private static final String[] TEST_COLUMN_NAMES = {"C1_VARCHAR", "C2#INTEGER", "3C_CHAR"};

  private static final String[] TEST_COLUMN_TYPES = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};

  private static final String[] TEST_COLUMN_ALL_TYPES = {"INTEGER", "BIGINT", "DOUBLE", "DECIMAL(10, 2)", "BOOLEAN", "TIMESTAMP", "BINARY", "VARCHAR(100)", "CHAR(100)"};

  private static final List<Object> TEST_COLUMN_ALL_TYPES_VALUES = Arrays.<Object>asList(10, 12345678910123L, 12.34, 456842.45, "TRUE", "2018-06-14 15:00:00.000", "abcdef", "testVarchar", "testChar");

  private static final Object[] EXPECTED_TEST_COLUMN_ALL_TYPES_VALUES = {10, 12345678910123L, 12.34, "456842.45", true, timeFromString("2018-06-14 15:00:00.000"), decodeHex("abcdef"), "testVarchar", "testChar"};

  private static final List<Object> TEST_COLUMN_VALUES = Arrays.<Object>asList("test", 42, "somestring");

  private static final List<Object> TEST_COLUMN_VALUES_MAPPED = Arrays.<Object>asList("test", "42", "somestring");

  private static final List<Object> TEST_COLUMN_VALUES_LINE2 = Arrays.<Object>asList("test2", 4242, "somestring2");

  private static HiveMiniCluster hiveMiniCluster;

  private static HiveServer2TestUtil hiveServer2TestUtil;

  @RunWith(Parameterized.class)
  @Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
  public static class ParquetCompressionCodecTestCase extends ImportJobTestCase {

    @Parameters(name = "compressionCodec = {0}")
    public static Iterable<? extends Object> authenticationParameters() {
      return Arrays.asList("snappy", "gzip");
    }

    @BeforeClass
    public static void beforeClass() {
      startHiveMiniCluster();
    }

    @AfterClass
    public static void afterClass() {
      stopHiveMiniCluster();
    }

    private final String compressionCodec;

    public ParquetCompressionCodecTestCase(String compressionCodec) {
      this.compressionCodec = compressionCodec;
    }

    @Override
    @Before
    public void setUp() {
      super.setUp();

      createTableWithColTypesAndNames(TEST_COLUMN_NAMES, TEST_COLUMN_TYPES, TEST_COLUMN_VALUES);
    }

    @Test
    public void testHiveImportAsParquetWithCompressionCodecCanBeLoaded() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("compression-codec", compressionCodec)
          .build();

      runImport(args);

      List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
      assertThat(rows, hasItems(TEST_COLUMN_VALUES));
    }

    @Test
    public void testImportedFilesHaveCorrectCodec() throws Exception {
      Path tablePath = new Path(hiveMiniCluster.getTempFolderPath() + "/" + getTableName().toLowerCase());
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("compression-codec", compressionCodec)
          .build();

      runImport(args);

      CompressionCodecName codec = new ParquetReader(tablePath).getCodec();
      assertEquals(compressionCodec, codec.name().toLowerCase());
    }
  }

  public static class GeneralParquetTestCase extends ImportJobTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
      startHiveMiniCluster();
    }

    @AfterClass
    public static void afterClass() {
      stopHiveMiniCluster();
    }

    @Override
    @Before
    public void setUp() {
      super.setUp();

      createTableWithColTypesAndNames(TEST_COLUMN_NAMES, TEST_COLUMN_TYPES, TEST_COLUMN_VALUES);
    }

    @Test
    public void testNormalHiveImportAsParquet() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName()).build();

      runImport(args);

      List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
      assertThat(rows, hasItems(TEST_COLUMN_VALUES));
    }

    @Test
    public void testHiveImportAsParquetWithMapColumnJavaAndOriginalColumnNameSucceeds() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("map-column-java", "C2#INTEGER=String")
          .build();

      runImport(args);

      List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
      assertThat(rows, hasItems(TEST_COLUMN_VALUES_MAPPED));
    }

    /**
     * This test case documents that the Avro identifier(C2_INTEGER)
     * of a special column name(C2#INTEGER) cannot be used in map-column-java.
     * The reason is that org.apache.sqoop.orm.AvroSchemaGenerator#toAvroType(java.lang.String, int)
     * which maps the Avro schema type uses the original column name and
     * not the Avro identifier but org.apache.sqoop.orm.ClassWriter#toJavaType(java.lang.String, int)
     * can map the DAO class field types based on the Avro identifier too so there will be a discrepancy
     * between the generated Avro schema types and the DAO class field types.
     */
    @Test
    public void testHiveImportAsParquetWithMapColumnJavaAndAvroIdentifierFails() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("map-column-java", "C2_INTEGER=String")
          .build();

      expectedException.expect(IOException.class);
      runImport(args);
    }

    /**
     * This test case documents that a mapping with the Avro identifier(C2_INTEGER)
     * of a special column name(C2#INTEGER) is ignored in map-column-hive.
     * The reason is that the column type of the Avro schema and the Hive table must
     * be equal and if we would be able to override the Hive column type using map-column-hive
     * the inconsistency would cause a Hive error during reading.
     */
    @Test
    public void testHiveImportAsParquetWithMapColumnHiveAndAvroIdentifierIgnoresMapping() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("map-column-hive", "C2_INTEGER=STRING")
          .build();

      runImport(args);

      List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
      assertThat(rows, hasItems(TEST_COLUMN_VALUES));
    }

    /**
     * This test case documents that the special column name(C2#INTEGER)
     * cannot be used in map-column-hive.
     * The reason is that Sqoop uses the Avro identifier(C2_INTEGER) as Hive column
     * name and there is a check in org.apache.sqoop.hive.TableDefWriter#getCreateTableStmt()
     * which verifies that all the columns in map-column-hive are actually valid column names.
     * Since C2_INTEGER is used instead of C2#INTEGER the check will fail on the latter.
     */
    @Test
    public void testHiveImportAsParquetWithMapColumnHiveAndOriginalColumnNameFails() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("map-column-hive", "C2#INTEGER=STRING")
          .build();

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("No column by the name C2#INTEGERfound while importing data");

      runImportThrowingException(args);
    }

    @Test
    public void testAllDataTypesHiveImportAsParquet() throws Exception {
      setCurTableName("all_datatypes_table");
      createTableWithColTypes(TEST_COLUMN_ALL_TYPES, TEST_COLUMN_ALL_TYPES_VALUES);
      String[] args = commonArgs(getConnectString(), getTableName()).build();

      runImport(args);

      // The result contains a byte[] so we have to use Arrays.deepEquals() to assert.
      Object[] firstRow = hiveServer2TestUtil.loadRawRowsFromTable(getTableName()).iterator().next().toArray();
      assertTrue(deepEquals(EXPECTED_TEST_COLUMN_ALL_TYPES_VALUES, firstRow));
    }

    @Test
    public void testAppendHiveImportAsParquet() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName()).build();

      runImport(args);

      insertIntoTable(TEST_COLUMN_NAMES, TEST_COLUMN_TYPES, TEST_COLUMN_VALUES_LINE2);

      runImport(args);

      List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
      assertThat(rows, hasItems(TEST_COLUMN_VALUES, TEST_COLUMN_VALUES_LINE2));
    }

    @Test
    public void testCreateOverwriteHiveImportAsParquet() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("hive-overwrite")
          .build();

      runImport(args);

      // Recreate the test table to contain different test data.
      dropTableIfExists(getTableName());
      createTableWithColTypesAndNames(TEST_COLUMN_NAMES, TEST_COLUMN_TYPES, TEST_COLUMN_VALUES_LINE2);

      runImport(args);

      List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
      assertEquals(asList(TEST_COLUMN_VALUES_LINE2), rows);
    }

    /**
     * --create-hive-table option is now supported with the Hadoop Parquet writer implementation.
     */
    @Test
    public void testCreateHiveImportAsParquet() throws Exception {
      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("create-hive-table")
          .build();

      runImport(args);

      expectedException.expectMessage("Error executing Hive import.");
      runImportThrowingException(args);
    }

    /**
     * This scenario works fine since the Hadoop Parquet writer implementation does not
     * check the Parquet schema of the existing files. The exception will be thrown
     * by Hive when it tries to read the files with different schema.
     */
    @Test
    public void testHiveImportAsParquetWhenTableExistsWithIncompatibleSchema() throws Exception {
      String hiveTableName = "hiveImportAsParquetWhenTableExistsWithIncompatibleSchema";
      String[] incompatibleSchemaTableTypes = {"INTEGER", "INTEGER", "INTEGER"};
      List<Object> incompatibleSchemaTableData = Arrays.<Object>asList(100, 200, 300);

      String[] args = commonArgs(getConnectString(), getTableName())
          .withOption("hive-table", hiveTableName)
          .build();

      runImport(args);

      // We make sure we create a new table in the test RDBMS.
      incrementTableNum();
      createTableWithColTypes(incompatibleSchemaTableTypes, incompatibleSchemaTableData);

      // Recreate the argument array to pick up the new RDBMS table name.
      args = commonArgs(getConnectString(), getTableName())
          .withOption("hive-table", hiveTableName)
          .build();

      runImport(args);
    }

  }

  private static ArgumentArrayBuilder commonArgs(String connectString, String tableName) {
    return new ArgumentArrayBuilder()
        .withProperty("parquetjob.configurator.implementation", "hadoop")
        .withOption("connect", connectString)
        .withOption("table", tableName)
        .withOption("hive-import")
        .withOption("hs2-url", hiveMiniCluster.getUrl())
        .withOption("num-mappers", "1")
        .withOption("as-parquetfile")
        .withOption("delete-target-dir");
  }

  public static void startHiveMiniCluster() {
    hiveMiniCluster = new HiveMiniCluster(new NoAuthenticationConfiguration());
    hiveMiniCluster.start();
    hiveServer2TestUtil = new HiveServer2TestUtil(hiveMiniCluster.getUrl());
  }

  public static void stopHiveMiniCluster() {
    hiveMiniCluster.stop();
  }

  private static byte[] decodeHex(String hexString) {
    try {
      return Hex.decodeHex(hexString.toCharArray());
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    }
  }
}
