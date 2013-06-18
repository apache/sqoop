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
package org.apache.sqoop.manager.sqlserver;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;
import org.apache.sqoop.manager.sqlserver.MSSQLTestDataFileParser.DATATYPES;
import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.orm.CompilationManager;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

/**
 * Test import delimited file from SQL Server.
 */
public class SQLServerDatatypeImportDelimitedFileManualTest
  extends SQLServerDatatypeImportSequenceFileManualTest {

/**
 * Create the argv to pass to Sqoop.
 *
 * @param includeHadoopFlags
 *            if true, then include -D various.settings=values
 * @param colNames
 *            the columns to import. If null, all columns are used.
 * @param conf
 *            a Configuration specifying additional properties to use when
 *            determining the arguments.
 * @return the argv as an array of strings.
*/
  protected String[] getArgv(boolean includeHadoopFlags, String[] colNames,
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

    args.add("--num-mappers");
    args.add("2");

    args.addAll(getExtraArgs(conf));

    return args.toArray(new String[0]);
  }


  private void runSqoopImport(String[] importCols) {
    Configuration conf = getConf();
      SqoopOptions opts = getSqoopOptions(conf);
      String username = MSSQLTestUtils.getDBUserName();
      String password = MSSQLTestUtils.getDBPassWord();
      opts.setUsername(username);
      opts.setPassword(password);

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
  }

  /**
  * Do a MapReduce-based import of the table and verify that the results were
  * imported as expected. (tests readFields(ResultSet) and toString())
  *
  * @param expectedVal
  *            the value we injected into the table.
  * @param importCols
  *            the columns to import. If null, all columns are used.
  */
  protected void verifyImport(String expectedVal, String[] importCols) {

    // paths to where our output file will wind up.
    Path tableDirPath = getTablePath();

    removeTableDir();

    runSqoopImport(importCols);
    Configuration conf = getConf();

    SqoopOptions opts = getSqoopOptions(conf);
    try {
      ImportTool importTool = new ImportTool();
      opts = importTool.parseArguments(getArgv(false, importCols, conf),
       conf, opts, true);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
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

      FileSystem fs = FileSystem.getLocal(conf);
      FileStatus[] stats = fs.listStatus(tableDirPath);

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
          String line;
          String fname = stat.getPath().toString();
          fname = fname.substring(5, fname.length());

          BufferedReader reader = new BufferedReader(
           new InputStreamReader(new FileInputStream(new File(
             fname))));
          try {
            line = reader.readLine();
            assertEquals(" expected a different string",
              expectedVal, line);
          } finally {
            IOUtils.closeStream(reader);
          }
          LOG.info("Read back from sequencefile: " + line);
          foundRecord = true;
          // Add trailing '\n' to expected value since
          // SqoopRecord.toString()
          // encodes the record delim.
          if (null == expectedVal) {
            assertEquals("Error validating result from SeqFile",
              "null\n", line);
          }
        } catch (EOFException eoe) {
          // EOF in a file isn't necessarily a problem. We may have
          // some
          // empty sequence files, which will throw this. Just
          // continue
          // in the loop.
        }
      }

      if (!foundRecord) {
        fail("Couldn't read any records from SequenceFiles");
      }
    } catch (IOException ioe) {
      LOG.error(StringUtils.stringifyException(ioe));
      fail("IOException: " + ioe.toString());
    } finally {
      if (null != prevClassLoader) {
      ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }


  public void testVarBinary() {
    if (!supportsVarBinary()) {
      return;
    }
    dataTypeTest(DATATYPES.VARBINARY);
  }

  public void testTime() {
    if (!supportsTime()) {
      skipped = true;
      return;
    }

    dataTypeTest(DATATYPES.TIME);
  }

  public String getResportFileName(){
    return this.getClass().toString()+".txt";
  }
}
