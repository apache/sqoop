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
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.SqoopOptions.InvalidOptionsException;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.orm.CompilationManager;
import org.apache.sqoop.testutil.CommonArgs;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.ReparseMapper;
import org.apache.sqoop.tool.ImportTool;
import org.apache.sqoop.util.ClassLoaderStack;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test that the parse() methods generated in user SqoopRecord implementations
 * work in SQL Server.
 *
 * This uses JDBC to import data from an SQLServer database to HDFS.
 *
 * Since this requires an SQLServer installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=SQLServerParseMethodsTest or -Dthirdparty=true.
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
public class SQLServerParseMethodsTest extends ImportJobTestCase {

  @Before
  public void setUp() {
    super.setUp();
    Path p = new Path(getWarehouseDir());
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      fs.delete(p);
    } catch (IOException e) {
      LOG.error("Setup fail with IOException: " + StringUtils.stringifyException(e));
      fail("Setup fail with IOException: " + StringUtils.stringifyException(e));
    }
  }

  @After
  public void tearDown() {
    try {
      dropTableIfExists(getTableName());
    } catch (SQLException sqle) {
      LOG.info("Table clean-up failed: " + sqle);
    } finally {
      super.tearDown();
    }
  }

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  private String[] getArgv(boolean includeHadoopFlags,
      String fieldTerminator, String lineTerminator, String encloser,
      String escape, boolean encloserRequired) {

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--as-textfile");
    args.add("--split-by");
    args.add("DATA_COL0"); // always split by first column.
    args.add("--fields-terminated-by");
    args.add(fieldTerminator);
    args.add("--lines-terminated-by");
    args.add(lineTerminator);
    args.add("--escaped-by");
    args.add(escape);
    if (encloserRequired) {
      args.add("--enclosed-by");
    } else {
      args.add("--optionally-enclosed-by");
    }
    args.add(encloser);
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  public void runParseTest(String fieldTerminator, String lineTerminator,
      String encloser, String escape, boolean encloseRequired)
      throws IOException {

    ClassLoader prevClassLoader = null;

    String[] argv = getArgv(true, fieldTerminator, lineTerminator,
        encloser, escape, encloseRequired);
    runImport(argv);
    try {
      String tableClassName = getTableName();

      argv = getArgv(false, fieldTerminator, lineTerminator, encloser,
          escape, encloseRequired);
      SqoopOptions opts = new ImportTool().parseArguments(argv, null,
          null, true);

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();

      // Make sure the user's class is loaded into our address space.
      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
          tableClassName);

      JobConf job = new JobConf();
      job.setJar(jarFileName);

      // Tell the job what class we're testing.
      job.set(ReparseMapper.USER_TYPE_NAME_KEY, tableClassName);

      // use local mode in the same JVM.
      ConfigurationHelper.setJobtrackerAddr(job, "local");
      job.set("fs.default.name", "file:///");

      String warehouseDir = getWarehouseDir();
      Path warehousePath = new Path(warehouseDir);
      Path inputPath = new Path(warehousePath, getTableName());
      Path outputPath = new Path(warehousePath, getTableName() + "-out");

      job.setMapperClass(ReparseMapper.class);
      job.setNumReduceTasks(0);
      FileInputFormat.addInputPath(job, inputPath);
      FileOutputFormat.setOutputPath(job, outputPath);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);

      JobClient.runJob(job);
    } catch (InvalidOptionsException ioe) {
      LOG.error(StringUtils.stringifyException(ioe));
      fail(ioe.toString());
    } catch (ParseException pe) {
      LOG.error(StringUtils.stringifyException(pe));
      fail(pe.toString());
    } finally {
      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  @Test
  public void testDefaults() throws IOException {
    String[] types = { "INTEGER", "VARCHAR(32)", "INTEGER" };
    String[] vals = { "64", "'foo'", "128" };

    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\"", "\\", false);
  }

  @Test
  public void testRequiredEnclose() throws IOException {
    String[] types = { "INTEGER", "VARCHAR(32)", "INTEGER" };
    String[] vals = { "64", "'foo'", "128" };

    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\"", "\\", true);
  }

  @Test
  public void testStringEscapes() throws IOException {
    String[] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)",
        "VARCHAR(32)", "VARCHAR(32)", };
    String[] vals = { "'foo'", "'foo,bar'", "'foo''bar'", "'foo\\bar'",
        "'foo,bar''baz'", };

    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\'", "\\", false);
  }

  @Test
  public void testNumericTypes() throws IOException {
    String[] types = { "INTEGER", "REAL", "FLOAT", "DATE", "TIME", "BIT", };
    String[] vals = { "42", "36.0", "127.1", "'2009-07-02'", "'11:24:00'",

    "1", };

    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\'", "\\", false);
  }

  protected boolean useHsqldbTestServer() {
    return false;
  }

  protected String getConnectString() {
    return MSSQLTestUtils.getDBConnectString();
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

    String username = MSSQLTestUtils.getDBUserName();
    String password = MSSQLTestUtils.getDBPassWord();
    SqoopOptions opts = new SqoopOptions(conf);
    opts.setUsername(username);
    opts.setPassword(password);
    return opts;

  }
}
