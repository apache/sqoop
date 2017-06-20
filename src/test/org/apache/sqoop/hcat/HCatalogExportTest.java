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

package org.apache.sqoop.hcat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.cloudera.sqoop.SqoopOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.hcat.HCatalogTestUtils.ColumnGenerator;
import org.apache.sqoop.hcat.HCatalogTestUtils.CreateMode;
import org.apache.sqoop.hcat.HCatalogTestUtils.KeyType;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.junit.Before;

import com.cloudera.sqoop.testutil.ExportJobTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that we can export HCatalog tables into databases.
 */
public class HCatalogExportTest extends ExportJobTestCase {
  private static final Log LOG =
    LogFactory.getLog(HCatalogExportTest.class);
  private HCatalogTestUtils utils = HCatalogTestUtils.instance();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    super.setUp();
    try {
      utils.initUtils();
    } catch (Exception e) {
      throw new RuntimeException("Error initializing HCatTestUtilis", e);
    }
  }
  /**
   * @return an argv for the CodeGenTool to use when creating tables to export.
   */
  protected String[] getCodeGenArgv(String... extraArgs) {
    List<String> codeGenArgv = new ArrayList<String>();

    if (null != extraArgs) {
      for (String arg : extraArgs) {
        codeGenArgv.add(arg);
      }
    }

    codeGenArgv.add("--table");
    codeGenArgv.add(getTableName());
    codeGenArgv.add("--connect");
    codeGenArgv.add(getConnectString());
    codeGenArgv.add("--hcatalog-table");
    codeGenArgv.add(getTableName());

    return codeGenArgv.toArray(new String[0]);
  }

  /**
   * Verify that for the max and min values of the 'id' column, the values for a
   * given column meet the expected values.
   */
  protected void assertColMinAndMax(String colName, ColumnGenerator generator)
    throws SQLException {
    Connection conn = getConnection();
    int minId = getMinRowId(conn);
    int maxId = getMaxRowId(conn);
    String table = getTableName();
    LOG.info("Checking min/max for column " + colName + " with type "
      + SqoopHCatUtilities.sqlTypeString(generator.getSqlType()));

    Object expectedMin = generator.getDBValue(minId);
    Object expectedMax = generator.getDBValue(maxId);

    utils.assertSqlColValForRowId(conn, table, minId, colName, false, expectedMin);
    utils.assertSqlColValForRowId(conn, table, maxId, colName, false, expectedMax);
  }

  protected void runHCatExport(List<String> addlArgsArray,
    final int totalRecords, String table,
    ColumnGenerator[] cols) throws Exception {
    utils.createHCatTable(CreateMode.CREATE_AND_LOAD,
      totalRecords, table, cols);
    utils.createSqlTable(getConnection(), true, totalRecords, table, false, cols);
    Map<String, String> addlArgsMap = utils.getAddlTestArgs();
    addlArgsArray.add("--verbose");
    addlArgsArray.add("-m");
    addlArgsArray.add("1");
    addlArgsArray.add("--hcatalog-table");
    addlArgsArray.add(table);
    String[] argv = {};

    if (addlArgsMap.containsKey("-libjars")) {
      argv = new String[2];
      argv[0] = "-libjars";
      argv[1] = addlArgsMap.get("-libjars");
    }
    for (String k : addlArgsMap.keySet()) {
      if (!k.equals("-libjars")) {
        addlArgsArray.add(k);
        addlArgsArray.add(addlArgsMap.get(k));
      }
    }
    String[] exportArgs = getArgv(true, 10, 10, newStrArray(argv,
      addlArgsArray.toArray(new String[0])));
    LOG.debug("Export args = " + Arrays.toString(exportArgs));
    SqoopHCatUtilities.instance().setConfigured(false);
    runExport(exportArgs);
    verifyExport(totalRecords);
    for (int i = 0; i < cols.length; i++) {
      assertColMinAndMax(cols[i].getName(), cols[i]);
    }
  }

  @Test
  public void testIntTypes() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "boolean", Types.BOOLEAN, HCatFieldSchema.Type.BOOLEAN, 0, 0,
        Boolean.TRUE, Boolean.TRUE, KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "tinyint", Types.INTEGER, HCatFieldSchema.Type.INT, 0, 0, 10,
        10, KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
        "smallint", Types.INTEGER, HCatFieldSchema.Type.INT, 0, 0, 100,
        100, KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(3),
        "int", Types.INTEGER, HCatFieldSchema.Type.INT, 0, 0, 1000,
        1000, KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(4),
        "bigint", Types.BIGINT, HCatFieldSchema.Type.BIGINT, 0, 0, 10000L,
        10000L, KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testExportWithColumnNameValue() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    String valueColumnThatHadIssuesWithClassWriter = "value";
    ColumnGenerator[] cols = new ColumnGenerator[] {
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
            "boolean", Types.BOOLEAN, HCatFieldSchema.Type.BOOLEAN, 0, 0,
            Boolean.TRUE, Boolean.TRUE, KeyType.NOT_A_KEY),
        HCatalogTestUtils.colGenerator(valueColumnThatHadIssuesWithClassWriter,
            "int", Types.INTEGER, HCatFieldSchema.Type.INT, 5, 5, 10,
            10, KeyType.NOT_A_KEY)
    };
    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testFloatTypes() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "float", Types.FLOAT, HCatFieldSchema.Type.FLOAT, 0, 0, 10.0F,
        10.F, KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "real", Types.FLOAT, HCatFieldSchema.Type.FLOAT, 0, 0, 20.0F,
        20.0F, KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
        "double", Types.DOUBLE, HCatFieldSchema.Type.DOUBLE, 0, 0, 30.0D,
        30.0D, KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testNumberTypes() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "numeric(18,2)", Types.NUMERIC, HCatFieldSchema.Type.STRING, 0, 0,
        "1000", new BigDecimal("1000"), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "decimal(18,2)", Types.DECIMAL, HCatFieldSchema.Type.STRING, 0, 0,
        "2000", new BigDecimal("2000"), KeyType.NOT_A_KEY),
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
          "decimal(18,2)", Types.DECIMAL, HCatFieldSchema.Type.DECIMAL, 18, 2,
          HiveDecimal.create(new BigDecimal("2000")),
          new BigDecimal("2000"), KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testDateTypes() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "date", Types.DATE, HCatFieldSchema.Type.STRING, 0, 0,
        "2013-12-31", new Date(113, 11, 31), KeyType.NOT_A_KEY),
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
          "date", Types.DATE, HCatFieldSchema.Type.DATE, 0, 0,
          new Date(113, 11, 31),
          new Date(113, 11, 31), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
        "time", Types.TIME, HCatFieldSchema.Type.STRING, 0, 0,
        "10:11:12", new Time(10, 11, 12), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(3),
        "timestamp", Types.TIMESTAMP, HCatFieldSchema.Type.STRING, 0, 0,
        "2013-12-31 10:11:12", new Timestamp(113, 11, 31, 10, 11, 12, 0),
        KeyType.NOT_A_KEY),
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(4),
          "timestamp", Types.TIMESTAMP, HCatFieldSchema.Type.TIMESTAMP, 0, 0,
          new Timestamp(113, 11, 31, 10, 11, 12, 0),
          new Timestamp(113, 11, 31, 10, 11, 12, 0),
          KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testDateTypesToBigInt() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    long offset = TimeZone.getDefault().getRawOffset();
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "date", Types.DATE, HCatFieldSchema.Type.BIGINT, 0, 0, 0 - offset,
        new Date(70, 0, 1), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "time", Types.TIME, HCatFieldSchema.Type.BIGINT, 0, 0,
        36672000L - offset, new Time(10, 11, 12), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
        "timestamp", Types.TIMESTAMP, HCatFieldSchema.Type.BIGINT, 0, 0,
        36672000L - offset, new Timestamp(70, 0, 1, 10, 11, 12, 0),
        KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--map-column-hive");
    addlArgsArray.add("COL0=bigint,COL1=bigint,COL2=bigint");
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testStringTypes() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "char(14)", Types.CHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "string to test", "string to test", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
          "char(14)", Types.CHAR, HCatFieldSchema.Type.CHAR, 14, 0,
          new HiveChar("string to test", 14), "string to test",
          KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
          "char(14)", Types.CHAR, HCatFieldSchema.Type.VARCHAR, 14, 0,
          new HiveVarchar("string to test", 14), "string to test",
          KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(3),
        "longvarchar", Types.LONGVARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "string to test", "string to test", KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testBinaryTypes() throws Exception {
    ByteBuffer bb = ByteBuffer.wrap(new byte[] { 0, 1, 2 });
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "binary(10)", Types.BINARY, HCatFieldSchema.Type.BINARY, 0, 0,
        bb.array(), bb.array(), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varbinary(10)", Types.BINARY, HCatFieldSchema.Type.BINARY, 0, 0,
        bb.array(), bb.array(), KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testColumnProjection() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "1", null, KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--columns");
    addlArgsArray.add("ID,MSG");
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);

  }

  @Test
  public void testStaticPartitioning() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "1", "1", KeyType.STATIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-partition-key");
    addlArgsArray.add("col0");
    addlArgsArray.add("--hive-partition-value");
    addlArgsArray.add("1");

    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testStaticPartitioningWithMultipleKeys() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "1", "1", KeyType.STATIC_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "2", "2", KeyType.STATIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hcatalog-partition-keys");
    addlArgsArray.add("col0,col1");
    addlArgsArray.add("--hcatalog-partition-values");
    addlArgsArray.add("1,2");

    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testDynamicPartitioning() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "1", "1", KeyType.DYNAMIC_KEY),
    };

    List<String> addlArgsArray = new ArrayList<String>();
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testStaticAndDynamicPartitioning() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "1", "1", KeyType.STATIC_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "2", "2", KeyType.DYNAMIC_KEY),
    };

    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-partition-key");
    addlArgsArray.add("col0");
    addlArgsArray.add("--hive-partition-value");
    addlArgsArray.add("1");
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testMultipleStaticKeysAndDynamicPartitioning() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "1", "1", KeyType.STATIC_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "2", "2", KeyType.STATIC_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "3", "3", KeyType.DYNAMIC_KEY),
    };

    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hcatalog-partition-keys");
    addlArgsArray.add("col0,col1");
    addlArgsArray.add("--hcatalog-partition-values");
    addlArgsArray.add("1,2");
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  /**
   * Test other file formats.
   */
  @Test
  public void testSequenceFile() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
            "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
            "1", "1", KeyType.STATIC_KEY),
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
            "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
            "2", "2", KeyType.DYNAMIC_KEY), };

    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-partition-key");
    addlArgsArray.add("col0");
    addlArgsArray.add("--hive-partition-value");
    addlArgsArray.add("1");
    utils.setStorageInfo(HCatalogTestUtils.STORED_AS_SEQFILE);
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testTextFile() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
            "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
            "1", "1", KeyType.STATIC_KEY),
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
            "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
            "2", "2", KeyType.DYNAMIC_KEY), };

    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-partition-key");
    addlArgsArray.add("col0");
    addlArgsArray.add("--hive-partition-value");
    addlArgsArray.add("1");
    utils.setStorageInfo(HCatalogTestUtils.STORED_AS_TEXT);
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
  }

  @Test
  public void testPublishExportJobData() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
            HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
                    "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
                    "1", "1", KeyType.STATIC_KEY),
            HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
                    "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
                    "2", "2", KeyType.DYNAMIC_KEY), };

    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("-D");
    addlArgsArray.add(ConfigurationConstants.DATA_PUBLISH_CLASS + "=" + DummyDataPublisher.class.getName());
    runHCatExport(addlArgsArray, TOTAL_RECORDS, table, cols);
    assert (DummyDataPublisher.storeTable.equals(getTableName()));
    assert (DummyDataPublisher.storeType.equals("hsqldb"));
    assert (DummyDataPublisher.operation.equals("export"));
  }

  @Test
  public void testWeCanTellIfHCatViewOrTable() throws Exception {
    String tableName = getTableName().toUpperCase();
    String viewName = "view";
    SqoopHCatUtilities utils = SqoopHCatUtilities.instance();
    createHCatTableAndView(tableName, viewName, utils);

    Configuration conf = getConf();
    conf.set("oraoop.disabled", "true");
    SqoopOptions opts = getSqoopOptions(conf);
    opts.setHCatTableName(tableName);

    assertFalse(utils.isHCatView(opts));
    opts.setHCatTableName(viewName);
    assertTrue(utils.isHCatView(opts));
  }

  @Test
  public void testExportHCatViewThrowsException() throws Exception {
    String tableName = getTableName().toUpperCase();
    String viewName = "view";
    SqoopHCatUtilities utils = SqoopHCatUtilities.instance();
    createHCatTableAndView(tableName, viewName, utils);

    ArrayList<String> args = new ArrayList<String>();
    args.add("--table");
    args.add(getTableName());
    args.add("--hcatalog-table");
    args.add(viewName);
    args.add("--connect");
    args.add(getConnectString());

    args.toArray(new String[0]);

    SqoopHCatUtilities.instance().setConfigured(false);
    exception.expect(java.io.IOException.class);
    runExport(args.toArray(new String[0]));
  }

  private void createHCatTableAndView(String tableName, String viewName, SqoopHCatUtilities utils) throws Exception {
    HCatalogTestUtils testUtils = HCatalogTestUtils.instance();
    ColumnGenerator[] cols = new ColumnGenerator[]{
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
            "boolean", Types.BOOLEAN, HCatFieldSchema.Type.BOOLEAN, 0, 0,
            Boolean.TRUE, Boolean.TRUE, KeyType.NOT_A_KEY),
        HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
            "int", Types.INTEGER, HCatFieldSchema.Type.INT, 5, 5, 10,
            10, KeyType.NOT_A_KEY)
    };
    testUtils.createHCatTable(CreateMode.CREATE_AND_LOAD, 10, tableName, cols);
    String createViewCmd = "drop view " + viewName + "; create view " + viewName + " as select * from " + tableName;
    utils.launchHCatCli(createViewCmd);
  }

}
