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
package org.apache.sqoop;

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
import org.apache.sqoop.tool.ImportAllTablesTool;
import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.testutil.ImportJobTestCase;

public class TestAutoResetMapper extends ImportJobTestCase {

  /** the names of the tables we're creating. */
  private List<String> tableNames;

  private String[][] expectedStrings;

  public static final Log LOG =
      LogFactory.getLog(TestAutoResetMapper.class.getName());

  private static  String[][] types = {
    { "INT NOT NULL", "VARCHAR(32)" },
    { "INT NOT NULL PRIMARY KEY", "VARCHAR(32)" },
    { "INT NOT NULL", "VARCHAR(32)" },
    { "INT NOT NULL PRIMARY KEY", "VARCHAR(32)" },
    { "INT NOT NULL", "VARCHAR(32)" },
  };

  private int[] expectedPartFiles = { 1, 2, 1, 2, 1};

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  private String [] getArgv() {
    ArrayList<String> args = new ArrayList<String>();

    args.add("-D");
    args.add("mapreduce.jobtracker.address=local");
    args.add("-D");
    args.add("fs.defaultFS=file:///");
    args.add("-D");
    args.add("jobclient.completion.poll.interval=50");
    args.add("-D");
    args.add("jobclient.progress.monitor.poll.interval=50");

    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--num-mappers");
    args.add("2");
    args.add("--connect");
    args.add(getConnectString());
    args.add("--escaped-by");
    args.add("\\");
    args.add("--autoreset-to-one-mapper");
    args.add("--verbose");

    return args.toArray(new String[0]);
  }

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

    int numTables = types.length;

    this.expectedStrings = new String[numTables][];

    int numRows = 2;

    for (int i = 0; i < numTables; ++i) {
      expectedStrings[i] = new String[numRows];
      List<String> vals = new ArrayList<String>();
      for (int j = 0; j < numRows; ++j) {
        String num = Integer.toString(j + 1);
        String str = "Table " + Integer.toString(i + 1) + " Row " + num;
        vals.add(num);
        vals.add("'" + str + "'");
        expectedStrings[i][j] = num + "," + str;
      }
      this.createTableWithColTypes(types[i], vals.toArray(new String[vals.size()]));
      this.tableNames.add(this.getTableName());
      this.removeTableDir();
      incrementTableNum();
    };

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

  public void testMultiTableImportWithAutoMapperReset() throws IOException {

    String[] argv = getArgv();
    runImport(new ImportAllTablesTool(), argv);

    Path warehousePath = new Path(this.getWarehouseDir());
    FileSystem fs = FileSystem.get(getConf());
    for (int i = 0; i < this.tableNames.size(); ++i) {
      String tableName = this.tableNames.get(i);
      LOG.debug("Validating import of " + tableName);
      Path tablePath = new Path(warehousePath, tableName);
      int numPartFiles = 0;
      List<String> importedData = new ArrayList<String>();
      // We expect utmost 2 files
      for (int m = 0; m < 2 ; ++m) {
        Path filePath = new Path(tablePath, "part-m-0000" + Integer.toString(m));
        if (fs.exists(filePath)) {
          ++numPartFiles;
          LOG.debug("Reading imported file " + filePath);
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
          String line = null;
          try {
            while ((line = reader.readLine()) != null) {
              importedData.add(line);
            }
          } finally {
            IOUtils.closeStream(reader);
          }
        }
      }
      assertEquals("Table " + tableName + " expected a different number of part files",
         expectedPartFiles[i], numPartFiles);
      for (int k = 0; k < this.expectedStrings[i].length; ++k) {
        assertEquals("Table " + tableName + "expected a different string",
          expectedStrings[i][k], importedData.get(k));
      }
    }
  }

}
