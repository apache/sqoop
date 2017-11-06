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
import org.apache.sqoop.manager.sqlserver.MSSQLTestDataFileParser.DATATYPES;
import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.orm.CompilationManager;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.util.ClassLoaderStack;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test to import delimited file from SQL Server.
 *
 * This uses JDBC to import data from an SQLServer database to HDFS.
 *
 * Since this requires an SQLServer installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=SQLServerDatatypeImportDelimitedFileTest or -Dthirdparty=true.
 *
 * You need to put SQL Server JDBC driver library (sqljdbc4.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons) and set it's path through -Dsqoop.thirdparty.lib.dir.
 *
 * To set up your test environment:
 *   Install SQL Server Express 2012
 *   Create a database SQOOPTEST
 *   Create a login SQOOPUSER with password PASSWORD and grant all
 *   access for SQOOPTEST to SQOOPUSER.
 *   Set these through -Dsqoop.test.sqlserver.connectstring.host_url, -Dsqoop.test.sqlserver.database and
 *   -Dms.sqlserver.password
 */
public class SQLServerDatatypeImportDelimitedFileTest
  extends SQLServerDatatypeImportSequenceFileTest {

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
    args.add(MSSQLTestUtils.getDBConnectString());

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
          LOG.info("Read back from delimited file: " + line);
          foundRecord = true;
          // Add trailing '\n' to expected value since
          // SqoopRecord.toString()
          // encodes the record delim.
          if (null == expectedVal) {
            assertEquals("Error validating result from delimited file",
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
        fail("Couldn't read any records from delimited file");
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

  @Test
  public void testTime() {
    if (!supportsTime()) {
      skipped = true;
      return;
    }

    dataTypeTest(DATATYPES.TIME);
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testVarBinary() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testBit() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testBit2() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testBit3() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testNChar() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testChar() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testVarchar() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testNVarchar() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testBinary() {
  }

  @Ignore("Ignored as used type is not supported for table splitting.")
  @Test
  public void testTimestamp3() {
  }

  public String getResportFileName(){
    return this.getClass().toString()+".txt";
  }
}
