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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.orm.CompilationManager;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.testutil.SeqFileReader;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

/**
 * Test that using multiple mapper splits works.
 */
public class SQLServerMultiMapsManualTest extends ImportJobTestCase {

  public void setUp() {
    super.setUp();
    MSSQLTestUtils utils = new MSSQLTestUtils();
    try {
      utils.createTableFromSQL(MSSQLTestUtils.CREATE_TALBE_LINEITEM);
      utils.populateLineItem();
    } catch (SQLException e) {
      LOG.error("Setup fail with SQLException: " + StringUtils.stringifyException(e));
      fail("Setup fail with SQLException: " + e.toString());
    }

  }

  public void tearDown() {
    super.tearDown();
    MSSQLTestUtils utils = new MSSQLTestUtils();
    try {
      utils.dropTableIfExists("TPCH1M_LINEITEM");
    } catch (SQLException e) {
      LOG.error("TeatDown fail with SQLException: " + StringUtils.stringifyException(e));
      fail("TearDown fail with SQLException: " + e.toString());
    }
  }

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected String[] getArgv(boolean includeHadoopFlags, String[] colNames,
      String splitByCol) {
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }
    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();

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
    args.add("--username");
    args.add(username);
    args.add("--password");
    args.add(password);
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("2");

    return args.toArray(new String[0]);
  }

  // this test just uses the two int table.

  /** @return a list of Path objects for each data file */
  protected List<Path> getDataFilePaths() throws IOException {
    List<Path> paths = new ArrayList<Path>();
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(conf);

    FileStatus[] stats = fs.listStatus(getTablePath(),
        new Utils.OutputFileUtils.OutputFilesFilter());

    for (FileStatus stat : stats) {
      paths.add(stat.getPath());
    }

    return paths;
  }

  /**
   * Given a comma-delimited list of integers, grab and parse the first int.
   *
   * @param str
   *            a comma-delimited list of values, the first of which is an
   *            int.
   * @return the first field in the string, cast to int
   */
  private int getFirstInt(String str) {
    String[] parts = str.split(",");
    return Integer.parseInt(parts[0]);
  }

  public void runMultiMapTest(String splitByCol, int expectedSum)
      throws IOException {

    String[] columns = MSSQLTestUtils.getColumns();
    ClassLoader prevClassLoader = null;
    SequenceFile.Reader reader = null;

    String[] argv = getArgv(true, columns, splitByCol);
    runImport(argv);
    try {
      ImportTool importTool = new ImportTool();
      SqoopOptions opts = importTool.parseArguments(getArgv(false,
          columns, splitByCol), null, null, true);
      String username = MSSQLTestUtils.getDBUserName();
      String password = MSSQLTestUtils.getDBPassWord();
      opts.setUsername(username);
      opts.setPassword(password);

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();

      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
          getTableName());

      List<Path> paths = getDataFilePaths();
      Configuration conf = new Configuration();
      int curSum = 0;

      // We expect multiple files. We need to open all the files and sum
      // up the
      // first column across all of them.
      for (Path p : paths) {
        reader = SeqFileReader.getSeqFileReader(p.toString());

        // here we can actually instantiate (k, v) pairs.
        Object key = ReflectionUtils.newInstance(reader.getKeyClass(),
            conf);
        Object val = ReflectionUtils.newInstance(
            reader.getValueClass(), conf);

        // We know that these values are two ints separated by a ','
        // character. Since this is all dynamic, though, we don't want
        // to
        // actually link against the class and use its methods. So we
        // just
        // parse this back into int fields manually. Sum them up and
        // ensure
        // that we get the expected total for the first column, to
        // verify that
        // we got all the results from the db into the file.

        // now sum up everything in the file.
        while (reader.next(key) != null) {
          reader.getCurrentValue(val);
          curSum += getFirstInt(val.toString());
        }

        IOUtils.closeStream(reader);
        reader = null;
      }

      assertEquals("Total sum of first db column mismatch", expectedSum,
          curSum);
    } catch (InvalidOptionsException ioe) {
      LOG.error(StringUtils.stringifyException(ioe));
      fail(ioe.toString());
    } catch (ParseException pe) {
      LOG.error(StringUtils.stringifyException(pe));
      fail(pe.toString());
    } finally {
      IOUtils.closeStream(reader);

      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void testSplitByFirstCol() throws IOException {
    runMultiMapTest("L_ORDERKEY", 10);
  }

  protected boolean useHsqldbTestServer() {
    return false;
  }

  protected String getConnectString() {
    return System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");
  }

  /**
   * Drop a table if it already exists in the database.
   *
   * @param table
   *            the name of the table to drop.
   * @throws SQLException
   *             if something goes wrong.
   */
  protected void dropTableIfExists(String table) throws SQLException {
    Connection conn = getManager().getConnection();
    String sqlStmt = "IF OBJECT_ID('" + table
        + "') IS NOT NULL  DROP TABLE " + table;
    PreparedStatement statement = conn.prepareStatement(sqlStmt,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opt = new SqoopOptions(conf);
    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();
    SqoopOptions opts = new SqoopOptions(conf);
    opts.setUsername(username);
    opts.setPassword(password);

    return opt;
  }

  SqoopOptions getSqoopOptions(String[] args, SqoopTool tool) {
    SqoopOptions opts = null;
    try {
      opts = tool.parseArguments(args, null, null, true);
      String username = MSSQLTestUtils.getDBUserName();
      String password = MSSQLTestUtils.getDBPassWord();
      opts.setUsername(username);
      opts.setPassword(password);

    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      fail("Invalid options: " + e.toString());
    }

    return opts;
  }

  protected String getTableName() {
    return "tpch1m_lineitem";
  }
}
