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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.manager.ImportJobContext;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.util.AppendUtils;

/**
 * Test that --append works.
 */
public class TestAppendUtils extends ImportJobTestCase {

  private static final int PARTITION_DIGITS = 5;
  private static final String FILEPART_SEPARATOR = "-";

  public static final Log LOG = LogFactory.getLog(TestAppendUtils.class
      .getName());

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected ArrayList getOutputlessArgv(boolean includeHadoopFlags, boolean queryBased,
      String[] colNames, Configuration conf) {
    if (null == colNames) {
      colNames = getColNames();
    }

    String splitByCol = colNames[0];
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    if(queryBased) {
      args.add("--query");
      args.add("SELECT * FROM " + getTableName() + " WHERE $CONDITIONS");
    } else {
      args.add("--table");
      args.add(getTableName());
    }
    args.add("--columns");
    args.add(columnsString);
    args.add("--split-by");
    args.add(splitByCol);
    args.add("--connect");
    args.add(getConnectString());
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("1");

    args.addAll(getExtraArgs(conf));

    return args;
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }

  /** the same than ImportJobTestCase but without removing tabledir. */
  protected void runUncleanImport(String[] argv) throws IOException {
    // run the tool through the normal entry-point.
    int ret;
    try {
      Configuration conf = getConf();
      SqoopOptions opts = getSqoopOptions(conf);
      Sqoop sqoop = new Sqoop(new ImportTool(), conf, opts);
      ret = Sqoop.runSqoop(sqoop, argv);
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: " + e.toString());
      e.printStackTrace();
      ret = 1;
    }

    // expect a successful return.
    if (0 != ret) {
      throw new IOException("Failure during job; return status " + ret);
    }
  }

  /** @return FileStatus for data files only. */
  private FileStatus[] listFiles(FileSystem fs, Path path) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(path);
    ArrayList files = new ArrayList();
    Pattern patt = Pattern.compile("part.*-([0-9][0-9][0-9][0-9][0-9]).*");
    for (FileStatus fstat : fileStatuses) {
      String fname = fstat.getPath().getName();
      if (!fstat.isDir()) {
        Matcher mat = patt.matcher(fname);
        if (mat.matches()) {
          files.add(fstat);
        }
      }
    }
    return (FileStatus[]) files.toArray(new FileStatus[files.size()]);
  }

  private class StatusPathComparator implements Comparator<FileStatus> {

    @Override
    public int compare(FileStatus fs1, FileStatus fs2) {
      return fs1.getPath().toString().compareTo(fs2.getPath().toString());
    }
  }

  /** @return a concat. string with file-creation dates excluding folders. */
  private String getFileCreationTimeImage(FileSystem fs, Path outputPath,
      int fileCount) throws IOException {
    // create string image with all file creation dates
    StringBuffer image = new StringBuffer();
    FileStatus[] fileStatuses = listFiles(fs, outputPath);
    // sort the file statuses by path so we have a stable order for
    // using 'fileCount'.
    Arrays.sort(fileStatuses, new StatusPathComparator());
    for (int i = 0; i < fileStatuses.length && i < fileCount; i++) {
      image.append(fileStatuses[i].getPath() + "="
          + fileStatuses[i].getModificationTime());
    }
    return image.toString();
  }

  /** @return the number part of a partition */
  private int getFilePartition(Path file) {
    String filename = file.getName();
    int pos = filename.lastIndexOf(FILEPART_SEPARATOR);
    if (pos != -1) {
      String part = filename.substring(pos + 1, pos + 1 + PARTITION_DIGITS);
      return Integer.parseInt(part);
    } else {
      return 0;
    }
  }

  /**
   * Test for ouput path file-count increase, current files untouched and new
   * correct partition number.
   *
   * @throws IOException
   */
  public void runAppendTest(ArrayList args, Path outputPath)
      throws IOException {

    try {

      // ensure non-existing output dir for insert phase
      FileSystem fs = FileSystem.get(getConf());
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }

      // run Sqoop in INSERT mode
      String[] argv = (String[]) args.toArray(new String[0]);
      runUncleanImport(argv);

      // get current file count
      FileStatus[] fileStatuses = listFiles(fs, outputPath);
      Arrays.sort(fileStatuses, new StatusPathComparator());
      int previousFileCount = fileStatuses.length;

      // get string image with all file creation dates
      String previousImage = getFileCreationTimeImage(fs, outputPath,
          previousFileCount);

      // get current last partition number
      Path lastFile = fileStatuses[fileStatuses.length - 1].getPath();
      int lastPartition = getFilePartition(lastFile);

      // run Sqoop in APPEND mode
      args.add("--append");
      argv = (String[]) args.toArray(new String[0]);
      runUncleanImport(argv);

      // check directory file increase
      fileStatuses = listFiles(fs, outputPath);
      Arrays.sort(fileStatuses, new StatusPathComparator());
      int currentFileCount = fileStatuses.length;
      assertTrue("Output directory didn't got increased in file count ",
          currentFileCount > previousFileCount);

      // check previous files weren't modified, also works for partition
      // overlapping
      String currentImage = getFileCreationTimeImage(fs, outputPath,
          previousFileCount);
      assertEquals("Previous files to appending operation were modified",
          currentImage, previousImage);

      // check that exists at least 1 new correlative partition
      // let's use a different way than the code being tested
      Path newFile = fileStatuses[previousFileCount].getPath(); // there is a
                                                                // new bound now
      int newPartition = getFilePartition(newFile);
      assertTrue("New partition file isn't correlative",
          lastPartition + 1 == newPartition);

    } catch (Exception e) {
      LOG.error("Got Exception: " + StringUtils.stringifyException(e));
      fail(e.toString());
    }
  }

  /** independent to target-dir. */
  public void testAppend() throws IOException {
    ArrayList args = getOutputlessArgv(false, false, HsqldbTestServer.getFieldNames(), getConf());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());

    Path output = new Path(getWarehouseDir(), HsqldbTestServer.getTableName());
    runAppendTest(args, output);
  }

  /** working with target-dir. */
  public void testAppendToTargetDir() throws IOException {
    ArrayList args = getOutputlessArgv(false, false, HsqldbTestServer.getFieldNames(), getConf());
    String targetDir = getWarehouseDir() + "/tempTargetDir";
    args.add("--target-dir");
    args.add(targetDir);

    // there's no need for a new param
    // in diff. w/--warehouse-dir there will no be $tablename dir
    Path output = new Path(targetDir);
    runAppendTest(args, output);
  }

  /**
   * Query based import should also work in append mode.
   *
   * @throws IOException
   */
  public void testAppendWithQuery() throws IOException {
    ArrayList args = getOutputlessArgv(false, true, HsqldbTestServer.getFieldNames(), getConf());
    String targetDir = getWarehouseDir() + "/tempTargetDir";
    args.add("--target-dir");
    args.add(targetDir);

    Path output = new Path(targetDir);
    runAppendTest(args, output);
  }

  /**
   * If the append source does not exist, don't crash.
   */
  public void testAppendSrcDoesNotExist() throws IOException {
    Configuration conf = new Configuration();
    if (!isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    SqoopOptions options = new SqoopOptions(conf);
    options.setTableName("meep");
    Path missingPath = new Path("doesNotExistForAnyReason");
    FileSystem local = FileSystem.getLocal(conf);
    assertFalse(local.exists(missingPath));
    ImportJobContext importContext = new ImportJobContext("meep", null,
        options, missingPath);
    AppendUtils utils = new AppendUtils(importContext);
    utils.append();
  }

}

