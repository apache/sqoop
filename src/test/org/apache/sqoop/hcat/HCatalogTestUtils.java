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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.junit.Assert;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;

/**
 * HCatalog common test utilities.
 *
 */
public final class HCatalogTestUtils {
  protected Configuration conf;
  private static List<HCatRecord> recsToLoad = new ArrayList<HCatRecord>();
  private static List<HCatRecord> recsRead = new ArrayList<HCatRecord>();
  private static final Log LOG = LogFactory.getLog(HCatalogTestUtils.class);
  private FileSystem fs;
  private final SqoopHCatUtilities utils = SqoopHCatUtilities.instance();
  private static final double DELTAVAL = 1e-10;
  public static final String SQOOP_HCATALOG_TEST_ARGS =
    "sqoop.hcatalog.test.args";
  private final boolean initialized = false;
  private static String storageInfo = null;
  public static final String STORED_AS_RCFILE = "stored as\n\trcfile\n";
  public static final String STORED_AS_SEQFILE = "stored as\n\tsequencefile\n";
  public static final String STORED_AS_TEXT = "stored as\n\ttextfile\n";

  private HCatalogTestUtils() {
  }

  private static final class Holder {
    @SuppressWarnings("synthetic-access")
    private static final HCatalogTestUtils INSTANCE = new HCatalogTestUtils();

    private Holder() {
    }
  }

  @SuppressWarnings("synthetic-access")
  public static HCatalogTestUtils instance() {
    return Holder.INSTANCE;
  }

  public static StringBuilder escHCatObj(String objectName) {
    return SqoopHCatUtilities.escHCatObj(objectName);
  }

  public void initUtils() throws IOException, MetaException {
    if (initialized) {
      return;
    }
    conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    fs = FileSystem.get(conf);
    fs.initialize(fs.getWorkingDirectory().toUri(), conf);
    storageInfo = null;
    SqoopHCatUtilities.setTestMode(true);
  }

  public static String getStorageInfo() {
    if (null != storageInfo && storageInfo.length() > 0) {
      return storageInfo;
    } else {
      return STORED_AS_RCFILE;
    }
  }

  public void setStorageInfo(String info) {
    storageInfo = info;
  }

  private static String getHCatDropTableCmd(final String dbName,
    final String tableName) {
    return "DROP TABLE IF EXISTS " + escHCatObj(dbName.toLowerCase()) + "."
      + escHCatObj(tableName.toLowerCase());
  }

  private static String getHCatCreateTableCmd(String dbName,
    String tableName, List<HCatFieldSchema> tableCols,
    List<HCatFieldSchema> partKeys) {
    StringBuilder sb = new StringBuilder();
    sb.append("create table ")
      .append(escHCatObj(dbName.toLowerCase()).append('.'));
    sb.append(escHCatObj(tableName.toLowerCase()).append(" (\n\t"));
    for (int i = 0; i < tableCols.size(); ++i) {
      HCatFieldSchema hfs = tableCols.get(i);
      if (i > 0) {
        sb.append(",\n\t");
      }
      sb.append(escHCatObj(hfs.getName().toLowerCase()));
      sb.append(' ').append(hfs.getTypeString());
    }
    sb.append(")\n");
    if (partKeys != null && partKeys.size() > 0) {
      sb.append("partitioned by (\n\t");
      for (int i = 0; i < partKeys.size(); ++i) {
        HCatFieldSchema hfs = partKeys.get(i);
        if (i > 0) {
          sb.append("\n\t,");
        }
        sb.append(escHCatObj(hfs.getName().toLowerCase()));
        sb.append(' ').append(hfs.getTypeString());
      }
      sb.append(")\n");
    }
    sb.append(getStorageInfo());
    LOG.info("Create table command : " + sb);
    return sb.toString();
  }

  /**
   * The record writer mapper for HCatalog tables that writes records from an in
   * memory list.
   */
  public void createHCatTableUsingSchema(String dbName,
    String tableName, List<HCatFieldSchema> tableCols,
    List<HCatFieldSchema> partKeys)
    throws Exception {

    String databaseName = dbName == null
      ? SqoopHCatUtilities.DEFHCATDB : dbName;
    LOG.info("Dropping HCatalog table if it exists " + databaseName
      + '.' + tableName);
    String dropCmd = getHCatDropTableCmd(databaseName, tableName);

    try {
      utils.launchHCatCli(dropCmd);
    } catch (Exception e) {
      LOG.debug("Drop hcatalog table exception : " + e);
      LOG.info("Unable to drop table." + dbName + "."
        + tableName + ".   Assuming it did not exist");
    }
    LOG.info("Creating HCatalog table if it exists " + databaseName
      + '.' + tableName);
    String createCmd = getHCatCreateTableCmd(databaseName, tableName,
      tableCols, partKeys);
    utils.launchHCatCli(createCmd);
    LOG.info("Created HCatalog table " + dbName + "." + tableName);
  }

  /**
   * The record writer mapper for HCatalog tables that writes records from an in
   * memory list.
   */
  public static class HCatWriterMapper extends
    Mapper<LongWritable, Text, BytesWritable, HCatRecord> {

    private static int writtenRecordCount = 0;

    public static int getWrittenRecordCount() {
      return writtenRecordCount;
    }

    public static void setWrittenRecordCount(int count) {
      HCatWriterMapper.writtenRecordCount = count;
    }

    @Override
    public void map(LongWritable key, Text value,
      Context context)
      throws IOException, InterruptedException {
      try {
        HCatRecord rec = recsToLoad.get(writtenRecordCount);
        context.write(null, rec);
        writtenRecordCount++;
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          e.printStackTrace(System.err);
        }
        throw new IOException(e);
      }
    }
  }

  /**
   * The record reader mapper for HCatalog tables that reads records into an in
   * memory list.
   */
  public static class HCatReaderMapper extends
    Mapper<WritableComparable, HCatRecord, BytesWritable, Text> {

    private static int readRecordCount = 0; // test will be in local mode

    public static int getReadRecordCount() {
      return readRecordCount;
    }

    public static void setReadRecordCount(int count) {
      HCatReaderMapper.readRecordCount = count;
    }

    @Override
    public void map(WritableComparable key, HCatRecord value,
      Context context) throws IOException, InterruptedException {
      try {
        recsRead.add(value);
        readRecordCount++;
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          e.printStackTrace(System.err);
        }
        throw new IOException(e);
      }
    }
  }

  private void createInputFile(Path path, int rowCount)
    throws IOException {
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    FSDataOutputStream os = fs.create(path);
    for (int i = 0; i < rowCount; i++) {
      String s = i + "\n";
      os.writeChars(s);
    }
    os.close();
  }

  public List<HCatRecord> loadHCatTable(String dbName,
    String tableName, Map<String, String> partKeyMap,
    HCatSchema tblSchema, List<HCatRecord> records)
    throws Exception {

    Job job = new Job(conf, "HCat load job");

    job.setJarByClass(this.getClass());
    job.setMapperClass(HCatWriterMapper.class);


    // Just writ 10 lines to the file to drive the mapper
    Path path = new Path(fs.getWorkingDirectory(),
      "mapreduce/HCatTableIndexInput");

    job.getConfiguration()
      .setInt(ConfigurationConstants.PROP_MAPRED_MAP_TASKS, 1);
    int writeCount = records.size();
    recsToLoad.clear();
    recsToLoad.addAll(records);
    createInputFile(path, writeCount);
    // input/output settings
    HCatWriterMapper.setWrittenRecordCount(0);

    FileInputFormat.setInputPaths(job, path);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(HCatOutputFormat.class);
    OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName, tableName,
      partKeyMap);

    HCatOutputFormat.setOutput(job, outputJobInfo);
    HCatOutputFormat.setSchema(job, tblSchema);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(DefaultHCatRecord.class);

    job.setNumReduceTasks(0);
    SqoopHCatUtilities.addJars(job, new SqoopOptions());
    boolean success = job.waitForCompletion(true);

    if (!success) {
      throw new IOException("Loading HCatalog table with test records failed");
    }
    utils.invokeOutputCommitterForLocalMode(job);
    LOG.info("Loaded " + HCatWriterMapper.writtenRecordCount + " records");
    return recsToLoad;
  }

  /**
   * Run a local map reduce job to read records from HCatalog table.
   * @param readCount
   * @param filter
   * @return
   * @throws Exception
   */
  public List<HCatRecord> readHCatRecords(String dbName,
    String tableName, String filter) throws Exception {

    HCatReaderMapper.setReadRecordCount(0);
    recsRead.clear();

    // Configuration conf = new Configuration();
    Job job = new Job(conf, "HCatalog reader job");
    job.setJarByClass(this.getClass());
    job.setMapperClass(HCatReaderMapper.class);
    job.getConfiguration()
      .setInt(ConfigurationConstants.PROP_MAPRED_MAP_TASKS, 1);
    // input/output settings
    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    HCatInputFormat.setInput(job, dbName, tableName).setFilter(filter);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    Path path = new Path(fs.getWorkingDirectory(),
      "mapreduce/HCatTableIndexOutput");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    FileOutputFormat.setOutputPath(job, path);

    job.waitForCompletion(true);
    LOG.info("Read " + HCatReaderMapper.readRecordCount + " records");

    return recsRead;
  }

  /**
   * An enumeration type to hold the partition key type of the ColumnGenerator
   * defined columns.
   */
  public enum KeyType {
    NOT_A_KEY,
    STATIC_KEY,
    DYNAMIC_KEY
  };

  /**
   * An enumeration type to hold the creation mode of the HCatalog table.
   */
  public enum CreateMode {
    NO_CREATION,
    CREATE,
    CREATE_AND_LOAD,
  };

  /**
   * When generating data for export tests, each column is generated according
   * to a ColumnGenerator.
   */
  public interface ColumnGenerator {
    /*
     * The column name
     */
    String getName();

    /**
     * For a row with id rowNum, what should we write into that HCatalog column
     * to export?
     */
    Object getHCatValue(int rowNum);

    /**
     * For a row with id rowNum, what should the database return for the given
     * column's value?
     */
    Object getDBValue(int rowNum);

    /** Return the column type to put in the CREATE TABLE statement. */
    String getDBTypeString();

    /** Return the SqlType for this column. */
    int getSqlType();

    /** Return the HCat type for this column. */
    HCatFieldSchema.Type getHCatType();

    /** Return the precision/length of the field if any. */
    int getHCatPrecision();

    /** Return the scale of the field if any. */
    int getHCatScale();

    /**
     * If the field is a partition key, then whether is part of the static
     * partitioning specification in imports or exports. Only one key can be a
     * static partitioning key. After the first column marked as static, rest of
     * the keys will be considered dynamic even if they are marked static.
     */
    KeyType getKeyType();
  }

  /**
   * Return the column name for a column index. Each table contains two columns
   * named 'id' and 'msg', and then an arbitrary number of additional columns
   * defined by ColumnGenerators. These columns are referenced by idx 0, 1, 2
   * and on.
   * @param idx
   *          the index of the ColumnGenerator in the array passed to
   *          createTable().
   * @return the name of the column
   */
  public static String forIdx(int idx) {
    return "col" + idx;
  }

  public static ColumnGenerator colGenerator(final String name,
    final String dbType, final int sqlType,
    final HCatFieldSchema.Type hCatType, final int hCatPrecision,
    final int hCatScale, final Object hCatValue,
    final Object dbValue, final KeyType keyType) {
    return new ColumnGenerator() {

      @Override
      public String getName() {
        return name;
      }

      @Override
      public Object getDBValue(int rowNum) {
        return dbValue;
      }

      @Override
      public Object getHCatValue(int rowNum) {
        return hCatValue;
      }

      @Override
      public String getDBTypeString() {
        return dbType;
      }

      @Override
      public int getSqlType() {
        return sqlType;
      }

      @Override
      public HCatFieldSchema.Type getHCatType() {
        return hCatType;
      }

      @Override
      public int getHCatPrecision() {
        return hCatPrecision;
      }

      @Override
      public int getHCatScale() {
        return hCatScale;
      }

      public KeyType getKeyType() {
        return keyType;
      }

    };
  }

  public static void assertEquals(Object expectedVal,
    Object actualVal) {

    if (expectedVal != null && expectedVal instanceof byte[]) {
      Assert
        .assertArrayEquals((byte[]) expectedVal, (byte[]) actualVal);
    } else {
      if (expectedVal instanceof Float) {
        if (actualVal instanceof Double) {
          Assert.assertEquals(((Float) expectedVal).floatValue(),
            ((Double) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
            .assertEquals("Got unexpected column value", expectedVal,
              actualVal);
        }
      } else if (expectedVal instanceof Double) {
        if (actualVal instanceof Float) {
          Assert.assertEquals(((Double) expectedVal).doubleValue(),
            ((Float) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
            .assertEquals("Got unexpected column value", expectedVal,
              actualVal);
        }
      } else if (expectedVal instanceof HiveVarchar) {
        HiveVarchar vc1 = (HiveVarchar) expectedVal;
        if (actualVal instanceof HiveVarchar) {
          HiveVarchar vc2 = (HiveVarchar)actualVal;
          assertEquals(vc1.getCharacterLength(), vc2.getCharacterLength());
          assertEquals(vc1.getValue(), vc2.getValue());
        } else {
          String vc2 = (String)actualVal;
          assertEquals(vc1.getCharacterLength(), vc2.length());
          assertEquals(vc1.getValue(), vc2);
        }
      } else if (expectedVal instanceof HiveChar) {
        HiveChar c1 = (HiveChar) expectedVal;
        if (actualVal instanceof HiveChar) {
          HiveChar c2 = (HiveChar)actualVal;
          assertEquals(c1.getCharacterLength(), c2.getCharacterLength());
          assertEquals(c1.getValue(), c2.getValue());
        } else {
          String c2 = (String) actualVal;
          assertEquals(c1.getCharacterLength(), c2.length());
          assertEquals(c1.getValue(), c2);
        }
      } else {
        Assert
          .assertEquals("Got unexpected column value", expectedVal,
            actualVal);
      }
    }
  }

  /**
   * Verify that on a given row, a column has a given value.
   *
   * @param id
   *          the id column specifying the row to test.
   */
  public void assertSqlColValForRowId(Connection conn,
    String table, int id, String colName,
    Object expectedVal) throws SQLException {
    LOG.info("Verifying column " + colName + " has value " + expectedVal);

    PreparedStatement statement = conn.prepareStatement(
      "SELECT " + colName + " FROM " + table + " WHERE id = " + id,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    Object actualVal = null;
    try {
      ResultSet rs = statement.executeQuery();
      try {
        rs.next();
        actualVal = rs.getObject(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    assertEquals(expectedVal, actualVal);
  }

  /**
   * Verify that on a given row, a column has a given value.
   *
   * @param id
   *          the id column specifying the row to test.
   */
  public static void assertHCatColValForRowId(List<HCatRecord> recs,
    HCatSchema schema, int id, String fieldName,
    Object expectedVal) throws IOException {
    LOG.info("Verifying field " + fieldName + " has value " + expectedVal);

    Object actualVal = null;
    for (HCatRecord rec : recs) {
      if (rec.getInteger("id", schema).equals(id)) {
        actualVal = rec.get(fieldName, schema);
        break;
      }
    }
    if (actualVal == null) {
      throw new IOException("No record found with id = " + id);
    }
    if (expectedVal != null && expectedVal instanceof byte[]) {
      Assert
        .assertArrayEquals((byte[]) expectedVal, (byte[]) actualVal);
    } else {
      if (expectedVal instanceof Float) {
        if (actualVal instanceof Double) {
          Assert.assertEquals(((Float) expectedVal).floatValue(),
            ((Double) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
            .assertEquals("Got unexpected column value", expectedVal,
              actualVal);
        }
      } else if (expectedVal instanceof Double) {
        if (actualVal instanceof Float) {
          Assert.assertEquals(((Double) expectedVal).doubleValue(),
            ((Float) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
            .assertEquals("Got unexpected column value", expectedVal,
              actualVal);
        }
      } else {
        Assert
          .assertEquals("Got unexpected column value", expectedVal,
            actualVal);
      }
    }
  }

  /**
   * Return a SQL statement that drops a table, if it exists.
   *
   * @param tableName
   *          the table to drop.
   * @return the SQL statement to drop that table.
   */
  public static String getSqlDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName;
  }

  public static String getSqlCreateTableStatement(String tableName,
    ColumnGenerator... extraCols) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(tableName);
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraCols) {
      sb.append(", " + forIdx(colNum++) + " " + gen.getDBTypeString());
    }
    sb.append(")");
    String cmd = sb.toString();
    LOG.debug("Generated SQL create table command : " + cmd);
    return cmd;
  }

  public static String getSqlInsertTableStatement(String tableName,
    ColumnGenerator... extraCols) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(tableName);
    sb.append(" (id, msg");
    int colNum = 0;
    for (int i = 0; i < extraCols.length; ++i) {
      sb.append(", " + forIdx(colNum++));
    }
    sb.append(") VALUES ( ?, ?");
    for (int i = 0; i < extraCols.length; ++i) {
      sb.append(",?");
    }
    sb.append(")");
    String s = sb.toString();
    LOG.debug("Generated SQL insert table command : " + s);
    return s;
  }

  public void createSqlTable(Connection conn, boolean generateOnly,
    int count, String table, ColumnGenerator... extraCols)
    throws Exception {
    PreparedStatement statement = conn.prepareStatement(
      getSqlDropTableStatement(table),
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } catch (SQLException sqle) {
      conn.rollback();
    } finally {
      statement.close();
    }
    statement = conn.prepareStatement(
      getSqlCreateTableStatement(table, extraCols),
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
    if (!generateOnly) {
      loadSqlTable(conn, table, count, extraCols);
    }
  }

  public HCatSchema createHCatTable(CreateMode mode, int count,
    String table, ColumnGenerator... extraCols)
    throws Exception {
    HCatSchema hCatTblSchema = generateHCatTableSchema(extraCols);
    HCatSchema hCatPartSchema = generateHCatPartitionSchema(extraCols);
    HCatSchema hCatFullSchema = new HCatSchema(hCatTblSchema.getFields());
    for (HCatFieldSchema hfs : hCatPartSchema.getFields()) {
      hCatFullSchema.append(hfs);
    }
    if (mode != CreateMode.NO_CREATION) {

      createHCatTableUsingSchema(null, table,
        hCatTblSchema.getFields(), hCatPartSchema.getFields());
      if (mode == CreateMode.CREATE_AND_LOAD) {
        HCatSchema hCatLoadSchema = new HCatSchema(hCatTblSchema.getFields());
        HCatSchema dynPartSchema =
          generateHCatDynamicPartitionSchema(extraCols);
        for (HCatFieldSchema hfs : dynPartSchema.getFields()) {
          hCatLoadSchema.append(hfs);
        }
        loadHCatTable(hCatLoadSchema, table, count, extraCols);
      }
    }
    return hCatFullSchema;
  }

  private void loadHCatTable(HCatSchema hCatSchema, String table,
    int count, ColumnGenerator... extraCols)
    throws Exception {
    Map<String, String> staticKeyMap = new HashMap<String, String>();
    for (ColumnGenerator col : extraCols) {
      if (col.getKeyType() == KeyType.STATIC_KEY) {
        staticKeyMap.put(col.getName(), (String) col.getHCatValue(0));
      }
    }
    loadHCatTable(null, table, staticKeyMap,
      hCatSchema, generateHCatRecords(count, hCatSchema, extraCols));
  }

  private void loadSqlTable(Connection conn, String table, int count,
    ColumnGenerator... extraCols) throws Exception {
    PreparedStatement statement = conn.prepareStatement(
      getSqlInsertTableStatement(table, extraCols),
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      for (int i = 0; i < count; ++i) {
        statement.setObject(1, i, Types.INTEGER);
        statement.setObject(2, "textfield" + i, Types.VARCHAR);
        for (int j = 0; j < extraCols.length; ++j) {
          statement.setObject(j + 3, extraCols[j].getDBValue(i),
            extraCols[j].getSqlType());
        }
        statement.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }
    } finally {
      statement.close();
    }
  }

  private HCatSchema generateHCatTableSchema(ColumnGenerator... extraCols)
    throws Exception {
    List<HCatFieldSchema> hCatTblCols = new ArrayList<HCatFieldSchema>();
    hCatTblCols.clear();
    PrimitiveTypeInfo tInfo;
    tInfo = new PrimitiveTypeInfo();
    tInfo.setTypeName(HCatFieldSchema.Type.INT.name().toLowerCase());
    hCatTblCols.add(new HCatFieldSchema("id", tInfo, ""));
    tInfo = new PrimitiveTypeInfo();
    tInfo.setTypeName(HCatFieldSchema.Type.STRING.name().toLowerCase());
    hCatTblCols
      .add(new HCatFieldSchema("msg", tInfo, ""));
    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() == KeyType.NOT_A_KEY) {
        switch(gen.getHCatType()) {
          case CHAR:
            tInfo = new CharTypeInfo(gen.getHCatPrecision());
            break;
          case VARCHAR:
            tInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            break;
          case DECIMAL:
            tInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
              gen.getHCatScale());
            break;
          default:
            tInfo = new PrimitiveTypeInfo();
            tInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            break;
        }
        hCatTblCols
          .add(new HCatFieldSchema(gen.getName(), tInfo, ""));
      }
    }
    HCatSchema hCatTblSchema = new HCatSchema(hCatTblCols);
    return hCatTblSchema;
  }

  private HCatSchema generateHCatPartitionSchema(ColumnGenerator... extraCols)
    throws Exception {
    List<HCatFieldSchema> hCatPartCols = new ArrayList<HCatFieldSchema>();
    PrimitiveTypeInfo tInfo;

    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() != KeyType.NOT_A_KEY) {
        switch(gen.getHCatType()) {
          case CHAR:
            tInfo = new CharTypeInfo(gen.getHCatPrecision());
            break;
          case VARCHAR:
            tInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            break;
          case DECIMAL:
            tInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
            gen.getHCatScale());
            break;
          default:
            tInfo = new PrimitiveTypeInfo();
            tInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            break;
        }
        hCatPartCols
          .add(new HCatFieldSchema(gen.getName(), tInfo, ""));
      }
    }
    HCatSchema hCatPartSchema = new HCatSchema(hCatPartCols);
    return hCatPartSchema;
  }

  private HCatSchema generateHCatDynamicPartitionSchema(
    ColumnGenerator... extraCols) throws Exception {
    List<HCatFieldSchema> hCatPartCols = new ArrayList<HCatFieldSchema>();
    PrimitiveTypeInfo tInfo;

    hCatPartCols.clear();
    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() != KeyType.NOT_A_KEY) {
        if (gen.getKeyType() == KeyType.STATIC_KEY) {
          continue;
        }
        switch(gen.getHCatType()) {
          case CHAR:
            tInfo = new CharTypeInfo(gen.getHCatPrecision());
            break;
          case VARCHAR:
            tInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            break;
          case DECIMAL:
            tInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
            gen.getHCatScale());
            break;
          default:
            tInfo = new PrimitiveTypeInfo();
            tInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            break;
        }
        hCatPartCols
          .add(new HCatFieldSchema(gen.getName(), tInfo, ""));
      }
    }
    HCatSchema hCatPartSchema = new HCatSchema(hCatPartCols);
    return hCatPartSchema;

  }

  private HCatSchema generateHCatStaticPartitionSchema(
    ColumnGenerator... extraCols) throws Exception {
    List<HCatFieldSchema> hCatPartCols = new ArrayList<HCatFieldSchema>();
    PrimitiveTypeInfo tInfo;

    hCatPartCols.clear();
    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() == KeyType.STATIC_KEY) {
        switch(gen.getHCatType()) {
          case CHAR:
            tInfo = new CharTypeInfo(gen.getHCatPrecision());
            break;
          case VARCHAR:
            tInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            break;
          case DECIMAL:
            tInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
            gen.getHCatScale());
            break;
          default:
            tInfo = new PrimitiveTypeInfo();
            tInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            break;
        }
        hCatPartCols
          .add(new HCatFieldSchema(gen.getName(), tInfo, ""));
        break;
      }
    }
    HCatSchema hCatPartSchema = new HCatSchema(hCatPartCols);
    return hCatPartSchema;
  }

  private List<HCatRecord> generateHCatRecords(int numRecords,
    HCatSchema hCatTblSchema, ColumnGenerator... extraCols) throws Exception {
    List<HCatRecord> records = new ArrayList<HCatRecord>();
    List<HCatFieldSchema> hCatTblCols = hCatTblSchema.getFields();
    int size = hCatTblCols.size();
    for (int i = 0; i < numRecords; ++i) {
      DefaultHCatRecord record = new DefaultHCatRecord(size);
      record.set(hCatTblCols.get(0).getName(), hCatTblSchema, i);
      record.set(hCatTblCols.get(1).getName(), hCatTblSchema, "textfield" + i);
      int idx = 0;
      for (int j = 0; j < extraCols.length; ++j) {
        if (extraCols[j].getKeyType() == KeyType.STATIC_KEY) {
          continue;
        }
        record.set(hCatTblCols.get(idx + 2).getName(), hCatTblSchema,
          extraCols[j].getHCatValue(i));
        ++idx;
      }

      records.add(record);
    }
    return records;
  }

  public String hCatRecordDump(List<HCatRecord> recs,
    HCatSchema schema) throws Exception {
    List<String> fields = schema.getFieldNames();
    int count = 0;
    StringBuilder sb = new StringBuilder(1024);
    for (HCatRecord rec : recs) {
      sb.append("HCat Record : " + ++count).append('\n');
      for (String field : fields) {
        sb.append('\t').append(field).append('=');
        sb.append(rec.get(field, schema)).append('\n');
        sb.append("\n\n");
      }
    }
    return sb.toString();
  }

  public Map<String, String> getAddlTestArgs() {
    String addlArgs = System.getProperty(SQOOP_HCATALOG_TEST_ARGS);
    Map<String, String> addlArgsMap = new HashMap<String, String>();
    if (addlArgs != null) {
      String[] argsArray = addlArgs.split(",");
      for (String s : argsArray) {
        String[] keyVal = s.split("=");
        if (keyVal.length == 2) {
          addlArgsMap.put(keyVal[0], keyVal[1]);
        } else {
          LOG.info("Ignoring malformed addl arg " + s);
        }
      }
    }
    return addlArgsMap;
  }
}
