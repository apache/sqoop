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
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.SqoopOptions.FileLayout;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.tool.CodeGenTool;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.MergeTool;
import com.cloudera.sqoop.util.ClassLoaderStack;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test that the merge tool works.
 */
public class TestMerge extends BaseSqoopTestCase {

  private static final Log LOG =
      LogFactory.getLog(TestMerge.class.getName());

  protected ConnManager manager;
  protected Connection conn;

  public static final String SOURCE_DB_URL = "jdbc:hsqldb:mem:merge";

  private static final List<List<Integer>> initRecords = Arrays
      .asList(Arrays.asList(new Integer(0), new Integer(0)),
          Arrays.asList(new Integer(1), new Integer(42)));

  private static final List<List<Integer>> newRecords = Arrays.asList(
      Arrays.asList(new Integer(1), new Integer(43)),
      Arrays.asList(new Integer(3), new Integer(313)));

  private static final List<List<Integer>> mergedRecords = Arrays.asList(
      Arrays.asList(new Integer(0), new Integer(0)),
      Arrays.asList(new Integer(1), new Integer(43)),
      Arrays.asList(new Integer(3), new Integer(313)));

  @Before
  public void setUp() {
    super.setUp();
    manager = getManager();
    try {
      conn = manager.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static final String TABLE_NAME = "MergeTable";
  private static final String OLD_PATH = "merge-old";
  private static final String NEW_PATH = "merge_new";
  private static final String FINAL_PATH = "merge_final";

  public Configuration newConf() {
    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    conf.set("mapred.job.tracker", "local");
    return conf;
  }

  /**
   * Create a SqoopOptions to connect to the manager.
   */
  public SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = new SqoopOptions(conf);
    options.setConnectString(HsqldbTestServer.getDbUrl());

    return options;
  }

  protected void createTable(List<List<Integer>> records) throws SQLException {
    PreparedStatement s = conn.prepareStatement("DROP TABLE \"" + TABLE_NAME + "\" IF EXISTS");
    try {
      s.executeUpdate();
    } finally {
      s.close();
    }

    s = conn.prepareStatement("CREATE TABLE \"" + TABLE_NAME
        + "\" (id INT NOT NULL PRIMARY KEY, val INT, LASTMOD timestamp)");
    try {
      s.executeUpdate();
    } finally {
      s.close();
    }

    for (List<Integer> record : records) {
      final String values = StringUtils.join(record, ", ");
      s = conn
          .prepareStatement("INSERT INTO \"" + TABLE_NAME + "\" VALUES (" + values + ", now())");
      try {
        s.executeUpdate();
      } finally {
        s.close();
      }
    }

    conn.commit();
  }

  @Test
  public void testTextFileMerge() throws Exception {
    runMergeTest(SqoopOptions.FileLayout.TextFile);
  }

  @Test
  public void testAvroFileMerge() throws Exception {
    runMergeTest(SqoopOptions.FileLayout.AvroDataFile);
  }

  public void runMergeTest(SqoopOptions.FileLayout fileLayout) throws Exception {
    createTable(initRecords);

    // Create a jar to use for the merging process; we'll load it
    // into the current thread CL for when this runs. This needs
    // to contain a different class name than used for the imports
    // due to classloaderstack issues in the same JVM.
    final String MERGE_CLASS_NAME = "ClassForMerging";
    SqoopOptions options = getSqoopOptions(newConf());
    options.setTableName(TABLE_NAME);
    options.setClassName(MERGE_CLASS_NAME);

    CodeGenTool codeGen = new CodeGenTool();
    Sqoop codeGenerator = new Sqoop(codeGen, options.getConf(), options);
    int ret = Sqoop.runSqoop(codeGenerator, new String[0]);
    if (0 != ret) {
      fail("Nonzero exit from codegen: " + ret);
    }

    List<String> jars = codeGen.getGeneratedJarFiles();
    String jarFileName = jars.get(0);

    // Now do the imports.
    importData(OLD_PATH, fileLayout);

    // Check that we got records that meet our expected values.
    checkData(OLD_PATH, initRecords, fileLayout);

    Thread.sleep(25);

    // Modify the data in the warehouse.
    createTable(newRecords);

    Thread.sleep(25);

    // Do another import, into the "new" dir.
    importData(NEW_PATH, fileLayout);

    checkData(NEW_PATH, newRecords, fileLayout);

    // Now merge the results!
    ClassLoaderStack.addJarFile(jarFileName, MERGE_CLASS_NAME);
    Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
    options = getSqoopOptions(newConf());
    options.setMergeOldPath(new Path(warehouse, OLD_PATH).toString());
    options.setMergeNewPath(new Path(warehouse, NEW_PATH).toString());
    options.setMergeKeyCol("ID");
    options.setTargetDir(new Path(warehouse, FINAL_PATH).toString());
    options.setClassName(MERGE_CLASS_NAME);
    options.setExistingJarName(jarFileName);

    MergeTool mergeTool = new MergeTool();
    Sqoop merger = new Sqoop(mergeTool, options.getConf(), options);
    ret = Sqoop.runSqoop(merger, new String[0]);
    if (0 != ret) {
      fail("Merge failed with exit code " + ret);
    }

    checkData(FINAL_PATH, mergedRecords, fileLayout);
  }

  private void checkData(String dataDir, List<List<Integer>> records,
      SqoopOptions.FileLayout fileLayout) throws Exception {
    for (List<Integer> record : records) {
      assertRecordStartsWith(record, dataDir, fileLayout);
    }
  }

  private boolean valueMatches(GenericRecord genericRecord, List<Integer> recordVals) {
    return recordVals.get(0).equals(genericRecord.get(0))
        && recordVals.get(1).equals(genericRecord.get(1));
  }

  private void importData(String targetDir, SqoopOptions.FileLayout fileLayout) {
    SqoopOptions options;
    options = getSqoopOptions(newConf());
    options.setTableName(TABLE_NAME);
    options.setNumMappers(1);
    options.setFileLayout(fileLayout);
    options.setDeleteMode(true);

    Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);
    options.setTargetDir(new Path(warehouse, targetDir).toString());

    ImportTool importTool = new ImportTool();
    Sqoop importer = new Sqoop(importTool, options.getConf(), options);
    int ret = Sqoop.runSqoop(importer, new String[0]);
    if (0 != ret) {
      fail("Initial import failed with exit code " + ret);
    }
  }

  /**
   * @return true if the file specified by path 'p' contains a line
   * that starts with 'prefix'
   */
  protected boolean checkTextFileForLine(FileSystem fs, Path p, List<Integer> record)
      throws IOException {
    final String prefix = StringUtils.join(record, ',');
    BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(p)));
    try {
      while (true) {
        String in = r.readLine();
        if (null == in) {
          break; // done with the file.
        }

        if (in.startsWith(prefix)) {
          return true;
        }
      }
    } finally {
      r.close();
    }

    return false;
  }

  private boolean checkAvroFileForLine(FileSystem fs, Path p, List<Integer> record)
      throws IOException {
    SeekableInput in = new FsInput(p, new Configuration());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    FileReader<GenericRecord> reader = DataFileReader.openReader(in, datumReader);
    reader.sync(0);

    while (reader.hasNext()) {
      if (valueMatches(reader.next(), record)) {
        return true;
      }
    }

    return false;
  }

  protected boolean checkFileForLine(FileSystem fs, Path p, SqoopOptions.FileLayout fileLayout,
      List<Integer> record) throws IOException {
    boolean result = false;
    switch (fileLayout) {
      case TextFile:
        result = checkTextFileForLine(fs, p, record);
        break;
      case AvroDataFile:
        result = checkAvroFileForLine(fs, p, record);
        break;
    }
    return result;
  }

  /**
   * Return true if there's a file in 'dirName' with a line that starts with
   * 'prefix'.
   */
  protected boolean recordStartsWith(List<Integer> record, String dirName,
      SqoopOptions.FileLayout fileLayout)
      throws Exception {
    Path warehousePath = new Path(LOCAL_WAREHOUSE_DIR);
    Path targetPath = new Path(warehousePath, dirName);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    FileStatus [] files = fs.listStatus(targetPath);

    if (null == files || files.length == 0) {
      fail("Got no import files!");
    }

    for (FileStatus stat : files) {
      Path p = stat.getPath();
      if (p.getName().startsWith("part-")) {
        if (checkFileForLine(fs, p, fileLayout, record)) {
          // We found the line. Nothing further to do.
          return true;
        }
      }
    }

    return false;
  }

  protected void assertRecordStartsWith(List<Integer> record, String dirName,
      SqoopOptions.FileLayout fileLayout) throws Exception {
    if (!recordStartsWith(record, dirName, fileLayout)) {
      fail("No record found that starts with [" + StringUtils.join(record, ", ") + "] in " + dirName);
    }
  }
}
