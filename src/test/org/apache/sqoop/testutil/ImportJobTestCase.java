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

package org.apache.sqoop.testutil;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.orm.CompilationManager;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.tool.ImportTool;
import org.apache.sqoop.util.ClassLoaderStack;
import org.junit.Before;

import static org.apache.sqoop.Sqoop.SQOOP_RETHROW_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Class that implements common methods required for tests which import data
 * from SQL into HDFS and verify correct import.
 */
public abstract class ImportJobTestCase extends BaseSqoopTestCase {

  public static final Log LOG = LogFactory.getLog(
      ImportJobTestCase.class.getName());

  @Before
  public void setUp() {
    super.setUp();
    removeTableDir();
  }

  protected String getTablePrefix() {
    return "IMPORT_TABLE_";
  }

  /**
   * @return a list of additional args to pass to the sqoop command line.
   */
  protected List<String> getExtraArgs(Configuration conf) {
    return new ArrayList<String>();
  }

  /**
   * Create the argv to pass to Sqoop.
   * @param includeHadoopFlags if true, then include -D various.settings=values
   * @param colNames the columns to import. If null, all columns are used.
   * @param conf a Configuration specifying additional properties to use when
   * determining the arguments.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags, String [] colNames,
      Configuration conf) {
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

    args.add("--table");
    args.add(getTableName());
    args.add("--columns");
    args.add(columnsString);
    args.add("--split-by");
    args.add(splitByCol);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("1");

    args.addAll(getExtraArgs(conf));

    return args.toArray(new String[0]);
  }

  /**
   * Do a MapReduce-based import of the table and verify that the results
   * were imported as expected. (tests readFields(ResultSet) and toString())
   * @param expectedVal the value we injected into the table.
   * @param importCols the columns to import. If null, all columns are used.
   */
  protected void verifyImport(String expectedVal, String [] importCols) {

    // paths to where our output file will wind up.
    Path tableDirPath = getTablePath();

    removeTableDir();

    Configuration conf = getConf();
    //Need to disable OraOop for existing tests
    conf.set("oraoop.disabled", "true");
    SqoopOptions opts = getSqoopOptions(conf);

    // run the tool through the normal entry-point.
    int ret;
    try {
      Sqoop importer = new Sqoop(new ImportTool(), conf, opts);
      ret = Sqoop.runSqoop(importer, getArgv(true, importCols, conf));
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: " + e.toString());
      throw new RuntimeException(e);
    }

    // expect a successful return.
    assertEquals("Failure during job", 0, ret);

    opts = getSqoopOptions(conf);
    try {
      ImportTool importTool = new ImportTool();
      opts = importTool.parseArguments(getArgv(false, importCols, conf), conf,
          opts, true);
    } catch (Exception e) {
      fail(e.toString());
    }

    CompilationManager compileMgr = new CompilationManager(opts);
    String jarFileName = compileMgr.getJarFilename();
    ClassLoader prevClassLoader = null;
    try {
      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
          getTableName());

      // Now open and check all part-files in the table path until we find
      // a non-empty one that we can verify contains the value.
      if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
        conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
      }
      FileSystem fs = FileSystem.get(conf);
      FileStatus [] stats = fs.listStatus(tableDirPath);

      if (stats == null || stats.length == 0) {
        fail("Error: no files in " + tableDirPath);
      }

      boolean foundRecord = false;
      for (FileStatus stat : stats) {
        if (!stat.getPath().getName().startsWith("part-")
            && !stat.getPath().getName().startsWith("data-")) {
          // This isn't a data file. Ignore it.
          continue;
        }

        try {
          Object readValue = SeqFileReader.getFirstValue(
              stat.getPath().toString());
          LOG.info("Read back from sequencefile: " + readValue);
          foundRecord = true;
          // Add trailing '\n' to expected value since SqoopRecord.toString()
          // encodes the record delim.
          if (null == expectedVal) {
            assertEquals("Error validating result from SeqFile", "null\n",
                readValue.toString());
          } else {
            assertEquals("Error validating result from SeqFile",
                expectedVal + "\n", readValue.toString());
          }
        } catch (EOFException eoe) {
          // EOF in a file isn't necessarily a problem. We may have some
          // empty sequence files, which will throw this. Just continue
          // in the loop.
        }
      }

      if (!foundRecord) {
        fail("Couldn't read any records from SequenceFiles");
      }
    } catch (IOException ioe) {
      fail("IOException: " + ioe.toString());
    } finally {
      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  /**
   * Run a MapReduce-based import (using the argv provided to control
   * execution).
   */
  protected void runImport(SqoopTool tool, String [] argv) throws IOException {
    // run the tool through the normal entry-point.
    int ret;
    try {
      ret = runSqoopTool(tool, argv, getSqoopOptions(getConf()));
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

  private int runSqoopTool(SqoopTool tool, String [] argv, SqoopOptions sqoopOptions) {
    Configuration conf = getConf();
    //Need to disable OraOop for existing tests
    conf.set("oraoop.disabled", "true");
    Sqoop sqoop = new Sqoop(tool, conf, sqoopOptions);

    return Sqoop.runSqoop(sqoop, argv);
  }

  protected int runImportThrowingException(SqoopTool tool, String [] argv) {
    String oldRethrowProperty = System.getProperty(SQOOP_RETHROW_PROPERTY);
    System.setProperty(SQOOP_RETHROW_PROPERTY, "true");

    SqoopOptions sqoopOptions = getSqoopOptions(getConf());
    sqoopOptions.setThrowOnError(true);

    try {
      return runSqoopTool(tool, argv, sqoopOptions);
    } finally {
      if (oldRethrowProperty == null) {
        System.clearProperty(SQOOP_RETHROW_PROPERTY);
      } else {
        System.setProperty(SQOOP_RETHROW_PROPERTY, oldRethrowProperty);
      }
    }
  }

  /** run an import using the default ImportTool. */
  protected void runImport(String [] argv) throws IOException {
    runImport(new ImportTool(), argv);
  }

  protected void runImportThrowingException(String [] argv) {
    runImportThrowingException(new ImportTool(), argv);
  }

}
