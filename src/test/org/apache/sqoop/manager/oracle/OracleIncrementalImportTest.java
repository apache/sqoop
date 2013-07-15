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

package org.apache.sqoop.manager.oracle;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.OracleUtils;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test free form query import with the Oracle db.
 */
public class OracleIncrementalImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      OracleIncrementalImportTest.class.getName());

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String getConnectString() {
    return OracleUtils.CONNECT_STRING;
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    OracleUtils.setOracleAuth(opts);
    return opts;
  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    OracleUtils.dropTable(table, getManager());
  }

  /** the names of the tables we're creating. */
  private List<String> tableNames;

  @Override
  public void tearDown() {
    // Clean up the database on our way out.
    for (String tableName : tableNames) {
      try {
        dropTableIfExists(tableName);
      } catch (SQLException e) {
        LOG.warn("Error trying to drop table '" + tableName
                 + "' on tearDown: " + e);
      }
    }
    super.tearDown();
  }

  /**
   * Create the argv to pass to Sqoop.
   * @param tableName tableName to be used.
   * @param connPropsFileName connection properties to use
   * @param checkColumnName name of the column to use for check-column
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(String tableName, String connPropsFileName,
      String checkColumnName) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--connect");
    args.add(getConnectString());
    args.add("--target-dir");
    args.add(getWarehouseDir());
    args.add("--num-mappers");
    args.add("1");
    args.add("--table");
    args.add(tableName);
    args.add("--incremental");
    args.add("lastmodified");
    args.add("--check-column");
    args.add(checkColumnName);
    args.add("--last-value");
    args.add("2000-01-01 01:01:01.0");
    args.add("--connection-param-file");
    args.add(connPropsFileName);

    return args.toArray(new String[0]);
  }

  /**
   * Create a tables with a date column.  Run incremental import on the table
   * with date column as check-column.
   */
  public void testIncrementalImportWithLastModified() throws IOException {
    tableNames = new ArrayList<String>();
    String [] types = { "INT", "VARCHAR(10)", "DATE", };
    String [] vals = {
        "1", "'old_data'",
        "TO_DATE('1999-01-01 11:11:11', 'YYYY-MM-DD HH24:MI:SS')",
        "2", "'new_data'",
        "TO_DATE('2000-11-11 23:23:23', 'YYYY-MM-DD HH24:MI:SS')", };
    String tableName = getTableName();
    tableNames.add(tableName);
    createTableWithColTypes(types, vals);
    // Some version of Oracle's jdbc drivers automatically convert date to
    // timestamp. Since we don't want this to happen for this test,
    // we must explicitly use a property file to control this behavior.
    String connPropsFileName = "connection.properties";
    createFileWithContent(connPropsFileName,
        "oracle.jdbc.mapDateToTimestamp=false");
    String[] args = getArgv(tableName, connPropsFileName, getColName(2));
    runImport(args);

    Path warehousePath = new Path(this.getWarehouseDir());
    Path filePath = new Path(warehousePath, "part-m-00000");
    String output = readLineFromPath(filePath);
    String expectedVal = "2,new_data,2000-11-11";
    assertEquals("Incremental import result expected a different string",
                 expectedVal, output);
  }

  private void createFileWithContent(String connPropsFileName,
      String fileContent) throws IOException {
    File file = new File(connPropsFileName);
    if(file.exists())
      file.delete();
    Writer writer = new BufferedWriter(new FileWriter(connPropsFileName));
    writer.write(fileContent);
    writer.close();
  }

  private String readLineFromPath(Path filePath) throws IOException {
    BufferedReader reader = null;
    if (!isOnPhysicalCluster()) {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(
          new File(filePath.toString()))));
    } else {
      FileSystem dfs = FileSystem.get(getConf());
      FSDataInputStream dis = dfs.open(filePath);
      reader = new BufferedReader(new InputStreamReader(dis));
    }
    String line = null;
    try {
      line = reader.readLine();
    } finally {
      IOUtils.closeStream(reader);
    }
    return line;
  }

}

