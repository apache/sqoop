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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Test free form query import.
 */
public class TestFreeFormQueryImport extends ImportJobTestCase {

  private Log log;

  public TestFreeFormQueryImport() {
    this.log = LogFactory.getLog(TestFreeFormQueryImport.class.getName());
  }

  /**
   * @return the Log object to use for reporting during this test
   */
  protected Log getLogger() {
    return log;
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
        log.warn("Error trying to drop table '" + tableName
            + "' on tearDown: " + e);
      }
    }
    super.tearDown();
  }

  /**
   * Create the argv to pass to Sqoop.
   * @param splitByCol column of the table used to split work.
   * @param query free form query to be used.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(String splitByCol, String query) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--connect");
    args.add(getConnectString());
    args.add("--target-dir");
    args.add(getWarehouseDir());
    args.add("--split-by");
    args.add(splitByCol);
    args.add("--num-mappers");
    args.add("2");
    args.add("--query");
    args.add(query);

    return args.toArray(new String[0]);
  }

  /**
   * Create two tables that share the common id column.  Run free-form query
   * import on the result table that is created by joining the two tables on
   * the id column.
   */
  public void testSimpleJoin() throws IOException {
    tableNames = new ArrayList<String>();

    String [] types1 = { "SMALLINT", };
    String [] vals1 = { "1", };
    String tableName1 = getTableName();
    createTableWithColTypes(types1, vals1);
    tableNames.add(tableName1);

    incrementTableNum();

    String [] types2 = { "SMALLINT", "VARCHAR(32)", };
    String [] vals2 = { "1", "'foo'", };
    String tableName2 = getTableName();
    createTableWithColTypes(types2, vals2);
    tableNames.add(tableName2);

    String query = "SELECT "
        + tableName1 + "." + getColName(0) + ", "
        + tableName2 + "." + getColName(1) + " "
        + "FROM " + tableName1 + " JOIN " + tableName2 + " ON ("
        + tableName1 + "." + getColName(0) + " = "
        + tableName2 + "." + getColName(0) + ") WHERE "
        + tableName1 + "." + getColName(0) + " < 3 AND $CONDITIONS";

    runImport(getArgv(tableName1 + "." + getColName(0), query));

    Path warehousePath = new Path(this.getWarehouseDir());
    Path filePath = new Path(warehousePath, "part-m-00000");
    String expectedVal = "1,foo";

    BufferedReader reader = null;
    if (!isOnPhysicalCluster()) {
      reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(
              new File(filePath.toString()))));
    } else {
      FileSystem dfs = FileSystem.get(getConf());
      FSDataInputStream dis = dfs.open(filePath);
      reader = new BufferedReader(new InputStreamReader(dis));
    }
    try {
      String line = reader.readLine();
      assertEquals("QueryResult expected a different string",
          expectedVal, line);
    } finally {
      IOUtils.closeStream(reader);
    }
  }
}
