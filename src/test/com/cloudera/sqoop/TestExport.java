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

package com.cloudera.sqoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.testutil.ExportJobTestCase;
import com.cloudera.sqoop.tool.CodeGenTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

/**
 * Test that we can export data from HDFS into databases.
 */
public class TestExport extends ExportJobTestCase {

  /**
   * @return an argv for the CodeGenTool to use when creating tables to export.
   */
  protected String [] getCodeGenArgv(String... extraArgs) {
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
    codeGenArgv.add("--fields-terminated-by");
    codeGenArgv.add("\\t");
    codeGenArgv.add("--lines-terminated-by");
    codeGenArgv.add("\\n");

    return codeGenArgv.toArray(new String[0]);
  }


  protected String getRecordLine(int recordNum, ColumnGenerator... extraCols) {
    String idStr = Integer.toString(recordNum);
    StringBuilder sb = new StringBuilder();

    sb.append(idStr);
    sb.append("\t");
    sb.append(getMsgPrefix());
    sb.append(idStr);
    for (ColumnGenerator gen : extraCols) {
      sb.append("\t");
      sb.append(gen.getExportText(recordNum));
    }
    sb.append("\n");

    return sb.toString();
  }

  /** When generating data for export tests, each column is generated
      according to a ColumnGenerator. Methods exist for determining
      what to put into text strings in the files to export, as well
      as what the string representation of the column as returned by
      the database should look like.
    */
  public interface ColumnGenerator {
    /** For a row with id rowNum, what should we write into that
        line of the text file to export?
      */
    String getExportText(int rowNum);

    /** For a row with id rowNum, what should the database return
        for the given column's value?
      */
    String getVerifyText(int rowNum);

    /** Return the column type to put in the CREATE TABLE statement. */
    String getType();
  }

  /**
   * Create a data file that gets exported to the db.
   * @param fileNum the number of the file (for multi-file export)
   * @param numRecords how many records to write to the file.
   * @param gzip is true if the file should be gzipped.
   */
  protected void createTextFile(int fileNum, int numRecords, boolean gzip,
      ColumnGenerator... extraCols) throws IOException {
    int startId = fileNum * numRecords;

    String ext = ".txt";
    if (gzip) {
      ext = ext + ".gz";
    }
    Path tablePath = getTablePath();
    Path filePath = new Path(tablePath, "part" + fileNum + ext);

    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(tablePath);
    OutputStream os = fs.create(filePath);
    if (gzip) {
      CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
      CompressionCodec codec = ccf.getCodec(filePath);
      os = codec.createOutputStream(os);
    }
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
    for (int i = 0; i < numRecords; i++) {
      w.write(getRecordLine(startId + i, extraCols));
    }
    w.close();
    os.close();

    if (gzip) {
      verifyCompressedFile(filePath, numRecords);
    }
  }

  private void verifyCompressedFile(Path f, int expectedNumLines)
      throws IOException {
    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);
    InputStream is = fs.open(f);
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(f);
    LOG.info("gzip check codec is " + codec);
    Decompressor decompressor = CodecPool.getDecompressor(codec);
    if (null == decompressor) {
      LOG.info("Verifying gzip sanity with null decompressor");
    } else {
      LOG.info("Verifying gzip sanity with decompressor: "
          + decompressor.toString());
    }
    is = codec.createInputStream(is, decompressor);
    BufferedReader r = new BufferedReader(new InputStreamReader(is));
    int numLines = 0;
    while (true) {
      String ln = r.readLine();
      if (ln == null) {
        break;
      }
      numLines++;
    }

    r.close();
    assertEquals("Did not read back correct number of lines",
        expectedNumLines, numLines);
    LOG.info("gzip sanity check returned " + numLines + " lines; ok.");
  }

  /**
   * Create a data file in SequenceFile format that gets exported to the db.
   * @param fileNum the number of the file (for multi-file export).
   * @param numRecords how many records to write to the file.
   * @param className the table class name to instantiate and populate
   *          for each record.
   */
  private void createSequenceFile(int fileNum, int numRecords, String className)
      throws IOException {

    try {
      // Instantiate the value record object via reflection.
      Class cls = Class.forName(className, true,
          Thread.currentThread().getContextClassLoader());
      SqoopRecord record = (SqoopRecord) ReflectionUtils.newInstance(
          cls, new Configuration());

      // Create the SequenceFile.
      Configuration conf = new Configuration();
      if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
        conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
      }
      FileSystem fs = FileSystem.get(conf);
      Path tablePath = getTablePath();
      Path filePath = new Path(tablePath, "part" + fileNum);
      fs.mkdirs(tablePath);
      SequenceFile.Writer w = SequenceFile.createWriter(
          fs, conf, filePath, LongWritable.class, cls);

      // Now write the data.
      int startId = fileNum * numRecords;
      for (int i = 0; i < numRecords; i++) {
        record.parse(getRecordLine(startId + i));
        w.append(new LongWritable(startId + i), record);
      }

      w.close();
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (RecordParser.ParseError pe) {
      throw new IOException(pe);
    }
  }

  /** Return the column name for a column index.
   *  Each table contains two columns named 'id' and 'msg', and then an
   *  arbitrary number of additional columns defined by ColumnGenerators.
   *  These columns are referenced by idx 0, 1, 2...
   *  @param idx the index of the ColumnGenerator in the array passed to
   *   createTable().
   *  @return the name of the column
   */
  protected String forIdx(int idx) {
    return "col" + idx;
  }

  /**
   * Return a SQL statement that drops a table, if it exists.
   * @param tableName the table to drop.
   * @return the SQL statement to drop that table.
   */
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName + " IF EXISTS";
  }

  /** Create the table definition to export to, removing any prior table.
      By specifying ColumnGenerator arguments, you can add extra columns
      to the table of arbitrary type.
   */
  public void createTable(ColumnGenerator... extraColumns) throws SQLException {
    Connection conn = getConnection();
    PreparedStatement statement = conn.prepareStatement(
        getDropTableStatement(getTableName()),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(getTableName());
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraColumns) {
      sb.append(", " + forIdx(colNum++) + " " + gen.getType());
    }
    sb.append(")");

    statement = conn.prepareStatement(sb.toString(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  /**
   * @return the name of the staging table to be used for testing.
   */
  public String getStagingTableName() {
    return getTableName() + "_STAGE";
  }

  /**
   * Creates the staging table.
   * @param extraColumns extra columns that go in the staging table
   * @throws SQLException if an error occurs during export
   */
  public void createStagingTable(ColumnGenerator... extraColumns)
    throws SQLException {
    String stageTableName = getStagingTableName();
    Connection conn = getConnection();
    PreparedStatement statement = conn.prepareStatement(
        getDropTableStatement(stageTableName),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(stageTableName);
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraColumns) {
      sb.append(", " + forIdx(colNum++) + " " + gen.getType());
    }
    sb.append(")");

    statement = conn.prepareStatement(sb.toString(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }

  }

  /** Removing an existing table directory from the filesystem. */
  private void removeTablePath() throws IOException {
    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);
    fs.delete(getTablePath(), true);
  }

  /** Verify that on a given row, a column has a given value.
   * @param id the id column specifying the row to test.
   */
  private void assertColValForRowId(int id, String colName, String expectedVal)
      throws SQLException {
    Connection conn = getConnection();
    LOG.info("Verifying column " + colName + " has value " + expectedVal);

    PreparedStatement statement = conn.prepareStatement(
        "SELECT " + colName + " FROM " + getTableName() + " WHERE id = " + id,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    String actualVal = null;
    try {
      ResultSet rs = statement.executeQuery();
      try {
        rs.next();
        actualVal = rs.getString(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    assertEquals("Got unexpected column value", expectedVal, actualVal);
  }

  /** Verify that for the max and min values of the 'id' column, the values
      for a given column meet the expected values.
   */
  protected void assertColMinAndMax(String colName, ColumnGenerator generator)
      throws SQLException {
    Connection conn = getConnection();
    int minId = getMinRowId(conn);
    int maxId = getMaxRowId(conn);

    LOG.info("Checking min/max for column " + colName + " with type "
        + generator.getType());

    String expectedMin = generator.getVerifyText(minId);
    String expectedMax = generator.getVerifyText(maxId);

    assertColValForRowId(minId, colName, expectedMin);
    assertColValForRowId(maxId, colName, expectedMax);
  }

  /**
   * Create a new string array with 'moreEntries' appended to the 'entries'
   * array.
   * @param entries initial entries in the array
   * @param moreEntries variable-length additional entries.
   * @return an array containing entries with all of moreEntries appended.
   */
  protected String [] newStrArray(String [] entries, String... moreEntries)  {
    if (null == moreEntries) {
      return entries;
    }

    if (null == entries) {
      entries = new String[0];
    }

    int newSize = entries.length + moreEntries.length;
    String [] out = new String [newSize];

    int i = 0;
    for (String e : entries) {
      out[i++] = e;
    }

    for (String e : moreEntries) {
      out[i++] = e;
    }

    return out;
  }

  protected void multiFileTest(int numFiles, int recordsPerMap, int numMaps,
      String... argv) throws IOException, SQLException {

    final int TOTAL_RECORDS = numFiles * recordsPerMap;

    try {
      LOG.info("Beginning test: numFiles=" + numFiles + "; recordsPerMap="
          + recordsPerMap + "; numMaps=" + numMaps);

      for (int i = 0; i < numFiles; i++) {
        createTextFile(i, recordsPerMap, false);
      }

      createTable();
      runExport(getArgv(true, 10, 10, newStrArray(argv, "-m", "" + numMaps)));
      verifyExport(TOTAL_RECORDS);
    } finally {
      LOG.info("multi-file test complete");
    }
  }

  /**
   * Run an "export" on an empty file.
   */
  public void testEmptyExport() throws IOException, SQLException {
    multiFileTest(1, 0, 1);
  }

  /** Export 10 rows, make sure they load in correctly. */
  public void testTextExport() throws IOException, SQLException {
    multiFileTest(1, 10, 1);
  }

  /** Make sure we can use CombineFileInputFormat to handle multiple
   * files in a single mapper.
   */
  public void testMultiFilesOneMapper() throws IOException, SQLException {
    multiFileTest(2, 10, 1);
  }

  /** Make sure we can use CombineFileInputFormat to handle multiple
   * files and multiple maps.
   */
  public void testMultiFilesMultiMaps() throws IOException, SQLException {
    multiFileTest(2, 10, 2);
  }

  /** Export 10 rows from gzipped text files. */
  public void testGzipExport() throws IOException, SQLException {

    LOG.info("Beginning gzip export test");

    final int TOTAL_RECORDS = 10;

    createTextFile(0, TOTAL_RECORDS, true);
    createTable();
    runExport(getArgv(true, 10, 10));
    verifyExport(TOTAL_RECORDS);
    LOG.info("Complete gzip export test");
  }

  /**
   * Ensure that we use multiple statements in a transaction.
   */
  public void testMultiStatement() throws IOException, SQLException {
    final int TOTAL_RECORDS = 20;
    createTextFile(0, TOTAL_RECORDS, true);
    createTable();
    runExport(getArgv(true, 10, 10));
    verifyExport(TOTAL_RECORDS);
  }

  /**
   * Ensure that we use multiple transactions in a single mapper.
   */
  public void testMultiTransaction() throws IOException, SQLException {
    final int TOTAL_RECORDS = 20;
    createTextFile(0, TOTAL_RECORDS, true);
    createTable();
    runExport(getArgv(true, 5, 2));
    verifyExport(TOTAL_RECORDS);
  }

  /**
   * Exercises the testMultiTransaction test with staging table specified.
   * @throws IOException
   * @throws SQLException
   */
  public void testMultiTransactionWithStaging()
    throws IOException, SQLException {
    final int TOTAL_RECORDS = 20;
    createTextFile(0, TOTAL_RECORDS, true);
    createTable();
    createStagingTable();
    runExport(getArgv(true, 5, 2, "--staging-table", getStagingTableName()));
    verifyExport(TOTAL_RECORDS);
  }

  /**
   * Ensure that when we don't force a commit with a statement cap,
   * it happens anyway.
   */
  public void testUnlimitedTransactionSize() throws IOException, SQLException {
    final int TOTAL_RECORDS = 20;
    createTextFile(0, TOTAL_RECORDS, true);
    createTable();
    runExport(getArgv(true, 5, -1));
    verifyExport(TOTAL_RECORDS);
  }

  /** Run 2 mappers, make sure all records load in correctly. */
  public void testMultiMapTextExport() throws IOException, SQLException {

    final int RECORDS_PER_MAP = 10;
    final int NUM_FILES = 2;

    for (int f = 0; f < NUM_FILES; f++) {
      createTextFile(f, RECORDS_PER_MAP, false);
    }

    createTable();
    runExport(getArgv(true, 10, 10));
    verifyExport(RECORDS_PER_MAP * NUM_FILES);
  }

  /**
   * Run 2 mappers with staging enabled,
   * make sure all records load in correctly.
   */
  public void testMultiMapTextExportWithStaging()
  throws IOException, SQLException {

    final int RECORDS_PER_MAP = 10;
    final int NUM_FILES = 2;

    for (int f = 0; f < NUM_FILES; f++) {
      createTextFile(f, RECORDS_PER_MAP, false);
    }

    createTable();
    createStagingTable();
    runExport(getArgv(true, 10, 10, "--staging-table", getStagingTableName()));
    verifyExport(RECORDS_PER_MAP * NUM_FILES);
  }

  /** Export some rows from a SequenceFile, make sure they import correctly. */
  public void testSequenceFileExport() throws Exception {

    final int TOTAL_RECORDS = 10;

    // First, generate class and jar files that represent the table
    // we're exporting to.
    LOG.info("Creating initial schema for SeqFile test");
    createTable();
    LOG.info("Generating code...");
    CodeGenTool codeGen = new CodeGenTool();
    String [] codeGenArgs = getCodeGenArgv();
    SqoopOptions options = codeGen.parseArguments(
        codeGenArgs, null, null, true);
    codeGen.validateOptions(options);
    int ret = codeGen.run(options);
    assertEquals(0, ret);
    List<String> generatedJars = codeGen.getGeneratedJarFiles();

    // Now, wipe the created table so we can export on top of it again.
    LOG.info("Resetting schema and data...");
    createTable();

    // Wipe the directory we use when creating files to export to ensure
    // it's ready for new SequenceFiles.
    removeTablePath();

    assertNotNull(generatedJars);
    assertEquals("Expected 1 generated jar file", 1, generatedJars.size());
    String jarFileName = generatedJars.get(0);
    // Sqoop generates jars named "foo.jar"; by default, this should contain a
    // class named 'foo'. Extract the class name.
    Path jarPath = new Path(jarFileName);
    String jarBaseName = jarPath.getName();
    assertTrue(jarBaseName.endsWith(".jar"));
    assertTrue(jarBaseName.length() > ".jar".length());
    String className = jarBaseName.substring(0, jarBaseName.length()
        - ".jar".length());

    LOG.info("Using jar filename: " + jarFileName);
    LOG.info("Using class name: " + className);

    ClassLoader prevClassLoader = null;

    try {
      if (null != jarFileName) {
        prevClassLoader = ClassLoaderStack.addJarFile(jarFileName, className);
      }

      // Now use this class and jar name to create a sequence file.
      LOG.info("Writing data to SequenceFiles");
      createSequenceFile(0, TOTAL_RECORDS, className);

      // Now run and verify the export.
      LOG.info("Exporting SequenceFile-based data");
      runExport(getArgv(true, 10, 10, "--class-name", className,
          "--jar-file", jarFileName));
      verifyExport(TOTAL_RECORDS);
    } finally {
      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void testIntCol() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    // generate a column equivalent to rownum.
    ColumnGenerator gen = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        return "" + rowNum;
      }
      public String getVerifyText(int rowNum) {
        return "" + rowNum;
      }
      public String getType() {
        return "INTEGER";
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, gen);
    createTable(gen);
    runExport(getArgv(true, 10, 10));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), gen);
  }

  /** @return the column type for a large integer */
  protected String getBigIntType() {
    return "BIGINT";
  }

  public void testBigIntCol() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    // generate a column that won't fit in a normal int.
    ColumnGenerator gen = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        long val = (long) rowNum * 1000000000;
        return "" + val;
      }
      public String getVerifyText(int rowNum) {
        long val = (long) rowNum * 1000000000;
        return "" + val;
      }
      public String getType() {
        return getBigIntType();
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, gen);
    createTable(gen);
    runExport(getArgv(true, 10, 10));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), gen);
  }

  protected String pad(int n) {
    if (n <= 9) {
      return "0" + n;
    } else {
      return String.valueOf(n);
    }
  }

  /**
   * Get a column generator for DATE columns.
   */
  protected ColumnGenerator getDateColumnGenerator() {
    return new ColumnGenerator() {
      public String getExportText(int rowNum) {
        int day = rowNum + 1;
        return "2009-10-" + pad(day);
      }
      public String getVerifyText(int rowNum) {
        int day = rowNum + 1;
        return "2009-10-" + pad(day);
      }
      public String getType() {
        return "DATE";
      }
    };
  }

  /**
   * Get a column generator for TIME columns.
   */
  protected ColumnGenerator getTimeColumnGenerator() {
    return new ColumnGenerator() {
      public String getExportText(int rowNum) {
        return "10:01:" + pad(rowNum);
      }
      public String getVerifyText(int rowNum) {
        return "10:01:" + pad(rowNum);
      }
      public String getType() {
        return "TIME";
      }
    };
  }

  public void testDatesAndTimes() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    ColumnGenerator genDate = getDateColumnGenerator();
    ColumnGenerator genTime = getTimeColumnGenerator();

    createTextFile(0, TOTAL_RECORDS, false, genDate, genTime);
    createTable(genDate, genTime);
    runExport(getArgv(true, 10, 10));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), genDate);
    assertColMinAndMax(forIdx(1), genTime);
  }

  public void testNumericTypes() throws IOException, SQLException {
    final int TOTAL_RECORDS = 9;

    // Check floating point values
    ColumnGenerator genFloat = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        double v = 3.141 * (double) (rowNum + 1);
        return "" + v;
      }
      public String getVerifyText(int rowNum) {
        double v = 3.141 * (double) (rowNum + 1);
        return "" + v;
      }
      public String getType() {
        return "FLOAT";
      }
    };

    // Check precise decimal placement. The first of ten
    // rows will be 2.7181; the last of ten rows will be
    // 2.71810.
    ColumnGenerator genNumeric = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        int digit = rowNum + 1;
        return "2.718" + digit;
      }
      public String getVerifyText(int rowNum) {
        int digit = rowNum + 1;
        return "2.718" + digit;
      }
      public String getType() {
        return "NUMERIC(6,4)";
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, genFloat, genNumeric);
    createTable(genFloat, genNumeric);
    runExport(getArgv(true, 10, 10));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), genFloat);
    assertColMinAndMax(forIdx(1), genNumeric);
  }

  public void testColumnsExport() throws IOException, SQLException {
    testColumnsExport("id,msg," + forIdx(0) + "," + forIdx(2));
  }

  /**
   * It's possible to change the column string that
   * {@link #testColumnsExport()} uses -  you might want to do
   * this if your database randomly generates column names, instead
   * of using the given ones (e.g. stored procedure parameter
   * names in H2)
   */
  protected void testColumnsExport(
      String columnsStr) throws IOException, SQLException {
    final int TOTAL_COLUMNS = 3;
    final int TOTAL_RECORDS = 10;

    // This class is used to generate a column whose entries have unique
    // sequential values that are computed as follows:
    //  (row number * total number of columns) + column number
    class MultiColumnGenerator implements ColumnGenerator {
      private int col;
      MultiColumnGenerator(int col) {
        this.col = col;
      }
      public String getExportText(int rowNum) {
        return new Integer(rowNum * TOTAL_COLUMNS + col).toString();
      }
      public String getVerifyText(int rowNum) {
        return new Integer(rowNum * TOTAL_COLUMNS + col).toString();
      }
      public String getType() {
        return "INTEGER";
      }
    }

    // This class is used to generate a null column.
    class NullColumnGenerator implements ColumnGenerator {
      public String getExportText(int rowNum) {
        return null;
      }
      public String getVerifyText(int rowNum) {
        return null;
      }
      public String getType() {
        return "INTEGER";
      }
    }

    // Generate an input file that only contains partial columns: col0
    // and col2. Export these columns to the DB w/ the --columns option.
    // Finally, verify that these two columns are exported properly. In
    // addition, verify that the values of col1 in the DB are all null
    // as they are not exported.
    ColumnGenerator gen0 = new MultiColumnGenerator(0);
    ColumnGenerator gen1 = new MultiColumnGenerator(1);
    ColumnGenerator gen2 = new MultiColumnGenerator(2);

    createTextFile(0, TOTAL_RECORDS, false, gen0, gen2);
    createTable(gen0, gen1, gen2);

    runExport(getArgv(true, 10, 10, "--columns", columnsStr));

    ColumnGenerator genNull = new NullColumnGenerator();

    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), gen0);
    assertColMinAndMax(forIdx(2), gen2);
    assertColMinAndMax(forIdx(1), genNull);
  }
}
