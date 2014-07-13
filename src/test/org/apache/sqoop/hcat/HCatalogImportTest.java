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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.sqoop.hcat.HCatalogTestUtils.ColumnGenerator;
import org.apache.sqoop.hcat.HCatalogTestUtils.CreateMode;
import org.apache.sqoop.hcat.HCatalogTestUtils.KeyType;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.junit.Before;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * Test that we can export HCatalog tables into databases.
 */
public class HCatalogImportTest extends ImportJobTestCase {
  private static final Log LOG =
    LogFactory.getLog(HCatalogImportTest.class);
  private final HCatalogTestUtils utils = HCatalogTestUtils.instance();
  private List<String> extraTestArgs = null;
  private List<String> configParams = null;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    try {
      utils.initUtils();
      extraTestArgs = new ArrayList<String>();
      configParams = new ArrayList<String>();
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

  protected void setExtraArgs(List<String> args) {
    extraTestArgs.clear();
    if (args != null && args.size() > 0) {
      extraTestArgs.addAll(args);
    }
  }

  private List<String> getConfigParams() {
    return configParams;
  }

  private void setConfigParams(List<String> params) {
    configParams.clear();
    if (params != null && params.size() > 0) {
      configParams.addAll(params);
    }
  }
  @Override
  protected List<String> getExtraArgs(Configuration conf) {
    List<String> addlArgsArray = new ArrayList<String>();
    if (extraTestArgs != null && extraTestArgs.size() > 0) {
      addlArgsArray.addAll(extraTestArgs);
    }
    Map<String, String> addlArgsMap = utils.getAddlTestArgs();
    addlArgsArray.add("-m");
    addlArgsArray.add("1");
    addlArgsArray.add("--hcatalog-table");
    addlArgsArray.add(getTableName());
    for (String k : addlArgsMap.keySet()) {
      if (!k.equals("-libjars")) {
        addlArgsArray.add(k);
        addlArgsArray.add(addlArgsMap.get(k));
      }
    }
    return addlArgsArray;
  }

  @Override
  protected String[] getArgv(boolean includeHadoopFlags, String[] colNames,
    Configuration conf) {
    if (null == colNames) {
      colNames = getColNames();
    }
    String columnsString = "";
    String splitByCol = null;
    if (colNames != null) {
      splitByCol = colNames[0];
      for (String col : colNames) {
        columnsString += col + ",";
      }
    }
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }
    args.addAll(getConfigParams());
    args.add("--table");
    args.add(getTableName());
    if (colNames != null) {
      args.add("--columns");
      args.add(columnsString);
      args.add("--split-by");
      args.add(splitByCol);
    }
    args.add("--hcatalog-table");
    args.add(getTableName());
    args.add("--connect");
    args.add(getConnectString());
    args.addAll(getExtraArgs(conf));

    return args.toArray(new String[0]);
  }

  protected String[] getQueryArgv(boolean includeHadoopFlags, String[] colNames,
    Configuration conf) {

    String columnsString = "";
    String splitByCol = null;
    if (colNames != null) {
      splitByCol = colNames[0];
      columnsString = colNames[0];
      for (int c = 1; c < colNames.length; ++c) {
        columnsString +=  "," + colNames[c];
      }
    }
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }
    args.addAll(getConfigParams());
    args.add("--query");
    StringBuilder query = new StringBuilder("select ");
    if (colNames != null) {
      query.append(columnsString);
    } else {
      query.append('*');
    }
    query.append(' ');
    query.append("from ").append(getTableName());
    query.append(" where $CONDITIONS");
    args.add(query.toString());
    if (colNames != null) {
      args.add("--split-by");
      args.add(splitByCol);
    }
    args.add("--hcatalog-table");
    args.add(getTableName());
    args.add("--connect");
    args.add(getConnectString());
    args.addAll(getExtraArgs(conf));

    return args.toArray(new String[0]);
  }

  private void validateHCatRecords(final List<HCatRecord> recs,
    final HCatSchema schema, int expectedCount,
    ColumnGenerator... cols) throws IOException {
    if (recs.size() != expectedCount) {
      fail("Expected records = " + expectedCount
        + ", actual = " + recs.size());
      return;
    }
    schema.getFieldNames();
    Collections.sort(recs, new Comparator<HCatRecord>()
    {
      @Override
      public int compare(HCatRecord hr1, HCatRecord hr2) {
        try {
          return hr1.getInteger("id", schema)
            - hr2.getInteger("id", schema);
        } catch (Exception e) {
          LOG.warn("Exception caught while sorting hcat records " + e);
        }
        return 0;
      }
    });

    Object expectedVal = null;
    Object actualVal = null;
    for (int i = 0; i < recs.size(); ++i) {
      HCatRecord rec = recs.get(i);
      expectedVal = i;
      actualVal = rec.get("id", schema);
      LOG.info("Validating field: id (expected = "
        + expectedVal + ", actual = " + actualVal + ")");
      HCatalogTestUtils.assertEquals(expectedVal, actualVal);
      expectedVal = "textfield" + i;
      actualVal = rec.get("msg", schema);
      LOG.info("Validating field: msg (expected = "
        + expectedVal + ", actual = " + actualVal + ")");
      HCatalogTestUtils.assertEquals(rec.get("msg", schema), "textfield" + i);
      for (ColumnGenerator col : cols) {
        String name = col.getName().toLowerCase();
        expectedVal = col.getHCatValue(i);
        actualVal = rec.get(name, schema);
        LOG.info("Validating field: " + name + " (expected = "
          + expectedVal + ", actual = " + actualVal + ")");
        HCatalogTestUtils.assertEquals(expectedVal, actualVal);
      }
    }
  }

  protected void runImport(SqoopTool tool, String[] argv) throws IOException {
    // run the tool through the normal entry-point.
    int ret;
    try {
      Configuration conf = getConf();
      SqoopOptions opts = getSqoopOptions(conf);
      Sqoop sqoop = new Sqoop(tool, conf, opts);
      ret = Sqoop.runSqoop(sqoop, argv);
    } catch (Exception e) {
      LOG.error("Got exception running import: " + e.toString());
      e.printStackTrace();
      ret = 1;
    }
    if (0 != ret) {
      throw new IOException("Import failure; return status " + ret);
    }
  }

  protected void runHCatImport(List<String> addlArgsArray,
    int totalRecords, String table, ColumnGenerator[] cols,
    String[] cNames) throws Exception {
    runHCatImport(addlArgsArray, totalRecords, table, cols,
      cNames, false, false);
  }

  protected void runHCatQueryImport(List<String> addlArgsArray,
    int totalRecords, String table, ColumnGenerator[] cols,
    String[] cNames) throws Exception {
    runHCatImport(addlArgsArray, totalRecords, table, cols,
      cNames, false, true);
  }

  protected void runHCatImport(List<String> addlArgsArray,
    int totalRecords, String table, ColumnGenerator[] cols,
    String[] cNames, boolean dontCreate, boolean isQuery) throws Exception {
    CreateMode mode = CreateMode.CREATE;
    if (dontCreate) {
      mode = CreateMode.NO_CREATION;
    }
    HCatSchema tblSchema =
      utils.createHCatTable(mode, totalRecords, table, cols);
    utils.createSqlTable(getConnection(), false, totalRecords, table, cols);
    addlArgsArray.add("-m");
    addlArgsArray.add("1");
    addlArgsArray.add("--hcatalog-table");
    addlArgsArray.add(table);
    String[] colNames = null;
    if (cNames != null) {
      colNames = cNames;
    } else {
      colNames = new String[2 + cols.length];
      colNames[0] = "ID";
      colNames[1] = "MSG";
      for (int i = 0; i < cols.length; ++i) {
        colNames[2 + i] = cols[i].getName().toUpperCase();
      }
    }
    String[] importArgs;
    if (isQuery) {
      importArgs = getQueryArgv(true, colNames, new Configuration());
    } else {
      importArgs = getArgv(true, colNames, new Configuration());
    }
    LOG.debug("Import args = " + Arrays.toString(importArgs));
    SqoopHCatUtilities.instance().setConfigured(false);
    runImport(new ImportTool(), importArgs);
    List<HCatRecord> recs = utils.readHCatRecords(null, table, null);
    LOG.debug("HCat records ");
    LOG.debug(utils.hCatRecordDump(recs, tblSchema));
    validateHCatRecords(recs, tblSchema, 10, cols);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testDateTypes() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "date", Types.DATE, HCatFieldSchema.Type.STRING, 0, 0, "2013-12-31",
        new Date(113, 11, 31), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
          "date", Types.DATE, HCatFieldSchema.Type.DATE, 0, 0,
          new Date(113, 11, 31),
          new Date(113, 11, 31), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
        "time", Types.TIME, HCatFieldSchema.Type.STRING, 0, 0, "10:11:12",
        new Time(10, 11, 12), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(3),
        "timestamp", Types.TIMESTAMP, HCatFieldSchema.Type.STRING, 0, 0,
        "2013-12-31 10:11:12.0", new Timestamp(113, 11, 31, 10, 11, 12, 0),
        KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(4),
          "timestamp", Types.TIMESTAMP, HCatFieldSchema.Type.TIMESTAMP, 0, 0,
          new Timestamp(113, 11, 31, 10, 11, 12, 0),
          new Timestamp(113, 11, 31, 10, 11, 12, 0),
          KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testBinaryTypes() throws Exception {
    ByteBuffer bb = ByteBuffer.wrap(new byte[] { 0, 1, 2 });
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "binary(10)", Types.BINARY, HCatFieldSchema.Type.BINARY, 0, 0,
        bb.array(), bb.array(), KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "longvarbinary", Types.BINARY, HCatFieldSchema.Type.BINARY, 0, 0,
        bb.array(), bb.array(), KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testColumnProjection() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        null, null, KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    List<String> cfgParams = new ArrayList<String>();
    cfgParams.add("-D");
    cfgParams.add(SqoopHCatUtilities.DEBUG_HCAT_IMPORT_MAPPER_PROP
      + "=true");
    setConfigParams(cfgParams);
    String[] colNames = new String[] { "ID", "MSG" };
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, colNames);
  }

  public void testColumnProjectionMissingPartKeys() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        null, null, KeyType.DYNAMIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    List<String> cfgParams = new ArrayList<String>();
    cfgParams.add("-D");
    cfgParams.add(SqoopHCatUtilities.DEBUG_HCAT_IMPORT_MAPPER_PROP
      + "=true");
    setConfigParams(cfgParams);
    String[] colNames = new String[] { "ID", "MSG" };
    try {
      runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, colNames);
      fail("Column projection with missing dynamic partition keys must fail");
    } catch (Throwable t) {
      LOG.info("Job fails as expected : " + t);
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      LOG.info("Exception stack trace = " + sw);
    }
  }
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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testDynamicPartitioning() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "1", "1", KeyType.DYNAMIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

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
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  /**
   * Test other file formats.
   */
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
    setExtraArgs(addlArgsArray);
    utils.setStorageInfo(HCatalogTestUtils.STORED_AS_SEQFILE);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testTextFile() throws Exception {
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
    setExtraArgs(addlArgsArray);
    utils.setStorageInfo(HCatalogTestUtils.STORED_AS_TEXT);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testTableCreation() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        new HiveVarchar("1", 20), "1", KeyType.STATIC_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        new HiveVarchar("2", 20), "2", KeyType.DYNAMIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--create-hcatalog-table");
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols,
      null, true, false);
  }

  public void testTableCreationWithPartition() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
       new HiveVarchar("1", 20), "1", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
       new HiveVarchar("2", 20), "2", KeyType.STATIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-partition-key");
    addlArgsArray.add("col1");
    addlArgsArray.add("--hive-partition-value");
    addlArgsArray.add("2");
    addlArgsArray.add("--create-hcatalog-table");
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null, true, false);
  }

  public void testTableCreationWithMultipleStaticPartKeys() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
        new HiveVarchar("1", 20), "1", KeyType.STATIC_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
        new HiveVarchar("2", 20), "2", KeyType.STATIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hcatalog-partition-keys");
    addlArgsArray.add("col0,col1");
    addlArgsArray.add("--hcatalog-partition-values");
    addlArgsArray.add("1,2");
    addlArgsArray.add("--create-hcatalog-table");
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null, true, false);
  }

  public void testTableCreationWithStorageStanza() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
        new HiveVarchar("1", 20), "1", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
        new HiveVarchar("2", 20), "2", KeyType.STATIC_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-partition-key");
    addlArgsArray.add("col1");
    addlArgsArray.add("--hive-partition-value");
    addlArgsArray.add("2");
    addlArgsArray.add("--create-hcatalog-table");
    addlArgsArray.add("--hcatalog-storage-stanza");
    addlArgsArray.add(HCatalogTestUtils.STORED_AS_TEXT);
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null, true, false);
  }

  public void testHiveDropDelims() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "Test", "\u0001\n\rTest", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "Test2", "\u0001\r\nTest2", KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-drop-import-delims");
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testHiveDelimsReplacement() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "^^^Test", "\u0001\n\rTest", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
        "^^^Test2", "\u0001\r\nTest2", KeyType.NOT_A_KEY),
    };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--hive-delims-replacement");
    addlArgsArray.add("^");
    setExtraArgs(addlArgsArray);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testDynamicKeyInMiddle() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0, "1",
        "1", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0, "2",
        "2", KeyType.DYNAMIC_KEY), };
    List<String> addlArgsArray = new ArrayList<String>();
    setExtraArgs(addlArgsArray);
    utils.setStorageInfo(HCatalogTestUtils.STORED_AS_SEQFILE);
    runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testQueryImport() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0, "1",
        "1", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.STRING, 0, 0, "2",
        "2", KeyType.DYNAMIC_KEY), };
    List<String> addlArgsArray = new ArrayList<String>();
    setExtraArgs(addlArgsArray);

    runHCatQueryImport(addlArgsArray, TOTAL_RECORDS, table, cols, null);
  }

  public void testCreateTableWithPreExistingTable() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
        new HiveVarchar("1", 20), "1", KeyType.NOT_A_KEY),
      HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
        "varchar(20)", Types.VARCHAR, HCatFieldSchema.Type.VARCHAR, 20, 0,
        new HiveVarchar("2", 20), "2", KeyType.DYNAMIC_KEY), };
    List<String> addlArgsArray = new ArrayList<String>();
    addlArgsArray.add("--create-hcatalog-table");
    setExtraArgs(addlArgsArray);
    try {
      // Precreate table
      utils.createHCatTable(CreateMode.CREATE, TOTAL_RECORDS, table, cols);
      runHCatImport(addlArgsArray, TOTAL_RECORDS, table, cols,
        null, true, false);
      fail("HCatalog job with --create-hcatalog-table and pre-existing"
        + " table should fail");
    } catch (Exception e) {
      LOG.debug("Caught expected exception while running "
        + " create-hcatalog-table with pre-existing table test", e);
    }
  }
}
