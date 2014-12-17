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

import java.util.List;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.tool.CodeGenTool;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.MergeTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

/**
 * Test that the merge tool works.
 */
public class TestMerge extends BaseSqoopTestCase {

  private static final Log LOG =
      LogFactory.getLog(TestMerge.class.getName());

  protected ConnManager manager;
  protected Connection conn;

  public static final String SOURCE_DB_URL = "jdbc:hsqldb:mem:merge";

  @Override
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

  protected void createTable() throws SQLException {
    PreparedStatement s = conn.prepareStatement("DROP TABLE \"" + TABLE_NAME + "\" IF EXISTS");
    try {
      s.executeUpdate();
    } finally {
      s.close();
    }

    s = conn.prepareStatement("CREATE TABLE \"" + TABLE_NAME + "\" (id INT NOT NULL PRIMARY KEY, val INT, lastmod TIMESTAMP)");
    try {
      s.executeUpdate();
    } finally {
      s.close();
    }

    s = conn.prepareStatement("INSERT INTO \"" + TABLE_NAME + "\" VALUES (0, 0, NOW())");
    try {
      s.executeUpdate();
    } finally {
      s.close();
    }

    s = conn.prepareStatement("INSERT INTO \"" + TABLE_NAME + "\" VALUES (1, 42, NOW())");
    try {
      s.executeUpdate();
    } finally {
      s.close();
    }

    conn.commit();
  }

  public void testMerge() throws Exception {
    createTable();

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

    Path warehouse = new Path(BaseSqoopTestCase.LOCAL_WAREHOUSE_DIR);

    options = getSqoopOptions(newConf());
    options.setTableName(TABLE_NAME);
    options.setNumMappers(1);

    // Do an import of this data into the "old" dataset.
    options.setTargetDir(new Path(warehouse, "merge-old").toString());
    options.setIncrementalMode(IncrementalMode.DateLastModified);
    options.setIncrementalTestColumn("LASTMOD");

    ImportTool importTool = new ImportTool();
    Sqoop importer = new Sqoop(importTool, options.getConf(), options);
    ret = Sqoop.runSqoop(importer, new String[0]);
    if (0 != ret) {
      fail("Initial import failed with exit code " + ret);
    }

    // Check that we got records that meet our expected values.
    assertRecordStartsWith("0,0,", "merge-old");
    assertRecordStartsWith("1,42,", "merge-old");

    long prevImportEnd = System.currentTimeMillis();

    Thread.sleep(25);

    // Modify the data in the warehouse.
    PreparedStatement s = conn.prepareStatement("UPDATE \"" + TABLE_NAME + "\" SET val=43, lastmod=NOW() WHERE id=1");
    try {
      s.executeUpdate();
      conn.commit();
    } finally {
      s.close();
    }

    s = conn.prepareStatement("INSERT INTO \"" + TABLE_NAME + "\" VALUES (3,313,NOW())");
    try {
      s.executeUpdate();
      conn.commit();
    } finally {
      s.close();
    }

    Thread.sleep(25);

    // Do another import, into the "new" dir.
    options = getSqoopOptions(newConf());
    options.setTableName(TABLE_NAME);
    options.setNumMappers(1);
    options.setTargetDir(new Path(warehouse, "merge-new").toString());
    options.setIncrementalMode(IncrementalMode.DateLastModified);
    options.setIncrementalTestColumn("LASTMOD");
    options.setIncrementalLastValue(new Timestamp(prevImportEnd).toString());

    importTool = new ImportTool();
    importer = new Sqoop(importTool, options.getConf(), options);
    ret = Sqoop.runSqoop(importer, new String[0]);
    if (0 != ret) {
      fail("Second import failed with exit code " + ret);
    }

    assertRecordStartsWith("1,43,", "merge-new");
    assertRecordStartsWith("3,313,", "merge-new");

    // Now merge the results!
    ClassLoaderStack.addJarFile(jarFileName, MERGE_CLASS_NAME);

    options = getSqoopOptions(newConf());
    options.setMergeOldPath(new Path(warehouse, "merge-old").toString());
    options.setMergeNewPath(new Path(warehouse, "merge-new").toString());
    options.setMergeKeyCol("ID");
    options.setTargetDir(new Path(warehouse, "merge-final").toString());
    options.setClassName(MERGE_CLASS_NAME);

    MergeTool mergeTool = new MergeTool();
    Sqoop merger = new Sqoop(mergeTool, options.getConf(), options);
    ret = Sqoop.runSqoop(merger, new String[0]);
    if (0 != ret) {
      fail("Merge failed with exit code " + ret);
    }

    assertRecordStartsWith("0,0,", "merge-final");
    assertRecordStartsWith("1,43,", "merge-final");
    assertRecordStartsWith("3,313,", "merge-final");
  }

  /**
   * @return true if the file specified by path 'p' contains a line
   * that starts with 'prefix'
   */
  protected boolean checkFileForLine(FileSystem fs, Path p, String prefix)
      throws IOException {
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

  /**
   * Return true if there's a file in 'dirName' with a line that starts with
   * 'prefix'.
   */
  protected boolean recordStartsWith(String prefix, String dirName)
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
        if (checkFileForLine(fs, p, prefix)) {
          // We found the line. Nothing further to do.
          return true;
        }
      }
    }

    return false;
  }

  protected void assertRecordStartsWith(String prefix, String dirName)
      throws Exception {
    if (!recordStartsWith(prefix, dirName)) {
      fail("No record found that starts with " + prefix + " in " + dirName);
    }
  }
}

