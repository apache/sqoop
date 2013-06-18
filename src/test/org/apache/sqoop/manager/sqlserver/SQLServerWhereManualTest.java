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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils.*;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.manager.JdbcMySQLExportTest;
import com.cloudera.sqoop.orm.CompilationManager;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.testutil.SeqFileReader;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.util.ClassLoaderStack;

/**
 * Test that --where works in Sqoop. Methods essentially copied out of the other
 * Test* classes.
 */
public class SQLServerWhereManualTest extends ImportJobTestCase {

  public void setUp(){
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

  public void tearDown(){
    super.tearDown();
    MSSQLTestUtils utils = new MSSQLTestUtils();
    try {
      utils.dropTableIfExists("TPCH1M_LINEITEM");
    } catch (SQLException e) {
      LOG.error("TearDown fail with SQLException: " + StringUtils.stringifyException(e));
      fail("TearDown fail with SQLException: " + e.toString());
    }
  }
 /**
  * Create the argv to pass to Sqoop.
  *
  * @return the argv as an array of strings.
  */
 protected String[] getArgv(boolean includeHadoopFlags, String[] colNames,
   String whereClause) {
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
  args.add("--where");
  args.add(whereClause);
  args.add("--split-by");
  args.add("L_ORDERKEY");
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
  args.add("1");

  return args.toArray(new String[0]);
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

 public void runWhereTest(String whereClause, String firstValStr,
   int numExpectedResults, int expectedSum) throws IOException {

  String[] columns = MSSQLTestUtils.getColumns();
  ClassLoader prevClassLoader = null;
  SequenceFile.Reader reader = null;

  String[] argv = getArgv(true, columns, whereClause);
  runImport(argv);
  try {
   String username = MSSQLTestUtils.getDBUserName();
   String password = MSSQLTestUtils.getDBPassWord();

   SqoopOptions opts = new ImportTool().parseArguments(getArgv(false,
     columns, whereClause), null, null, true);
   opts.setUsername(username);
   opts.setPassword(password);
   CompilationManager compileMgr = new CompilationManager(opts);
   String jarFileName = compileMgr.getJarFilename();

   prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
     getTableName());

   reader = SeqFileReader.getSeqFileReader(getDataFilePath()
     .toString());

   // here we can actually instantiate (k, v) pairs.
   Configuration conf = new Configuration();
   Object key = ReflectionUtils
     .newInstance(reader.getKeyClass(), conf);
   Object val = ReflectionUtils.newInstance(reader.getValueClass(),
     conf);

   if (reader.next(key) == null) {
    fail("Empty SequenceFile during import");
   }

   // make sure that the value we think should be at the top, is.
   reader.getCurrentValue(val);
   assertEquals("Invalid ordering within sorted SeqFile", firstValStr,
     val.toString());

   // We know that these values are two ints separated by a ','
   // character.
   // Since this is all dynamic, though, we don't want to actually link
   // against the class and use its methods. So we just parse this back
   // into int fields manually. Sum them up and ensure that we get the
   // expected total for the first column, to verify that we got all
   // the
   // results from the db into the file.
   int curSum = getFirstInt(val.toString());
   int totalResults = 1;

   // now sum up everything else in the file.
   while (reader.next(key) != null) {
    reader.getCurrentValue(val);
    curSum += getFirstInt(val.toString());
    totalResults++;
   }

   assertEquals("Total sum of first db column mismatch", expectedSum,
     curSum);
   assertEquals("Incorrect number of results for query",
     numExpectedResults, totalResults);
  } catch (InvalidOptionsException ioe) {
   fail(ioe.toString());
  } catch (ParseException pe) {
   fail(pe.toString());
  } finally {
   IOUtils.closeStream(reader);

   if (null != prevClassLoader) {
    ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
   }
  }
 }

 public void testSingleClauseWhere() throws IOException {
  String whereClause = "L_ORDERKEY > 0 ";
  runWhereTest(whereClause,
    "1,2,3,4,5,6.00,7.00,8.00,AB,CD,abcd,efgh,hijk,dothis,likethis,nocomments"
      + "\n", 4, 10);
 }

 public void testMultiClauseWhere() throws IOException {
  String whereClause = "L_ORDERKEY > 1 AND L_PARTKEY < 4";
  runWhereTest(whereClause,
    "2,3,4,5,6,7.00,8.00,9.00,AB,CD,abcd,efgh,hijk,dothis,likethis,nocomments"
      + "\n", 1, 2);
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

 protected String getTableName() {
  return "tpch1m_lineitem";
 }
}
