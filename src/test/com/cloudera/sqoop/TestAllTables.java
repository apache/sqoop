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

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.After;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.ImportAllTablesTool;

/**
 * Test the --all-tables functionality that can import multiple tables.
 */
public class TestAllTables extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  private String [] getArgv(boolean includeHadoopFlags, String[] excludeTables) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--num-mappers");
    args.add("1");
    args.add("--escaped-by");
    args.add("\\");
    if (excludeTables != null) {
      args.add("--exclude-tables");
      args.add(StringUtils.join(excludeTables, ","));
    }

    return args.toArray(new String[0]);
  }

  /** the names of the tables we're creating. */
  private List<String> tableNames;

  /** The strings to inject in the (ordered) tables. */
  private List<String> expectedStrings;

  @Before
  public void setUp() {
    // start the server
    super.setUp();

    if (useHsqldbTestServer()) {
      // throw away TWOINTTABLE and things we don't care about.
      try {
        this.getTestServer().dropExistingSchema();
      } catch (SQLException sqlE) {
        fail(sqlE.toString());
      }
    }

    this.tableNames = new ArrayList<String>();
    this.expectedStrings = new ArrayList<String>();

    // create two tables.
    this.expectedStrings.add("A winner");
    this.expectedStrings.add("is you!");
    this.expectedStrings.add(null);

    int i = 0;
    for (String expectedStr: this.expectedStrings) {
      String wrappedStr = null;
      if (expectedStr != null) {
        wrappedStr = "'" + expectedStr + "'";
      }

      String [] types = { "INT NOT NULL PRIMARY KEY", "VARCHAR(32)" };
      String [] vals = { Integer.toString(i++) , wrappedStr };
      this.createTableWithColTypes(types, vals);
      this.tableNames.add(this.getTableName());
      this.removeTableDir();
      incrementTableNum();
    }
  }

  @After
  public void tearDown() {
    try {
      for (String table : tableNames) {
        dropTableIfExists(table);
      }
    } catch(SQLException e) {
      LOG.error("Can't clean up the database:", e);
    }
    super.tearDown();
  }

  public void testMultiTableImport() throws IOException {
    String [] argv = getArgv(true, null);
    runImport(new ImportAllTablesTool(), argv);

    Path warehousePath = new Path(this.getWarehouseDir());
    int i = 0;
    for (String tableName : this.tableNames) {
      Path tablePath = new Path(warehousePath, tableName);
      Path filePath = new Path(tablePath, "part-m-00000");

      // dequeue the expected value for this table. This
      // list has the same order as the tableNames list.
      String expectedVal = Integer.toString(i++) + ","
          + this.expectedStrings.get(0);
      this.expectedStrings.remove(0);

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
        assertEquals("Table " + tableName + " expected a different string",
            expectedVal, line);
      } finally {
        IOUtils.closeStream(reader);
      }
    }
  }

  public void testMultiTableImportWithExclude() throws IOException {
    String exclude = this.tableNames.get(0);
    String [] argv = getArgv(true, new String[]{ exclude });
    runImport(new ImportAllTablesTool(), argv);

    Path warehousePath = new Path(this.getWarehouseDir());
    int i = 0;
    for (String tableName : this.tableNames) {
      Path tablePath = new Path(warehousePath, tableName);
      Path filePath = new Path(tablePath, "part-m-00000");

      // dequeue the expected value for this table. This
      // list has the same order as the tableNames list.
      String expectedVal = Integer.toString(i++) + ","
          + this.expectedStrings.get(0);
      this.expectedStrings.remove(0);

      BufferedReader reader = null;
      if (!isOnPhysicalCluster()) {
        reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(
                new File(filePath.toString()))));
      } else {
        FSDataInputStream dis;
        FileSystem dfs = FileSystem.get(getConf());
        if (tableName.equals(exclude)) {
          try {
            dis = dfs.open(filePath);
            assertFalse(true);
          } catch (FileNotFoundException e) {
            // Success
            continue;
          }
        } else {
          dis = dfs.open(filePath);
        }
        reader = new BufferedReader(new InputStreamReader(dis));
      }
      try {
        String line = reader.readLine();
        assertEquals("Table " + tableName + " expected a different string",
            expectedVal, line);
      } finally {
        IOUtils.closeStream(reader);
      }
    }
  }
}
