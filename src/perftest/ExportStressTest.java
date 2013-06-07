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

import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * Stress test export procedure by running a large-scale export to MySQL.
 * This requires MySQL be configured with a database that can be accessed by
 * the specified username without a password. The user must be able to create
 * and drop tables in the database.
 *
 * Run with: src/scripts/run-perftest.sh ExportStressTest \
 *     (connect-str) (username)
 */
public class ExportStressTest extends Configured implements Tool {

  // Export 10 GB of data. Each record is ~100 bytes.
  public static final int NUM_FILES = 10;
  public static final int RECORDS_PER_FILE = 10 * 1024 * 1024;

  public static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public ExportStressTest() {
  }

  public void createFile(int fileId) throws IOException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path("ExportStressTest");
    fs.mkdirs(dirPath);
    Path filePath = new Path(dirPath, "input-" + fileId);

    OutputStream os = fs.create(filePath);
    Writer w = new BufferedWriter(new OutputStreamWriter(os));
    for (int i = 0; i < RECORDS_PER_FILE; i++) {
      long v = (long) i + ((long) RECORDS_PER_FILE * (long) fileId);
      w.write("" + v + "," + ALPHABET + ALPHABET + ALPHABET + ALPHABET + "\n");

    }
    w.close();
    os.close();
  }

  /** Create a set of data files to export. */
  public void createData() throws IOException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path("ExportStressTest");
    if (fs.exists(dirPath)) {
      System.out.println(
          "Export directory appears to already exist. Skipping data-gen.");
      return;
    }

    for (int i = 0; i < NUM_FILES; i++) {
      createFile(i);
    }
  }

  /** Create a table to hold our results. Drop any existing definition. */
  public void createTable(String connectStr, String username) throws Exception {
    Class.forName("com.mysql.jdbc.Driver"); // Load mysql driver.

    Connection conn = DriverManager.getConnection(connectStr, username, null);
    conn.setAutoCommit(false);
    PreparedStatement stmt = conn.prepareStatement(
        "DROP TABLE IF EXISTS ExportStressTestTable",
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
    stmt.executeUpdate();
    stmt.close();

    stmt = conn.prepareStatement(
        "CREATE TABLE ExportStressTestTable(id INT NOT NULL PRIMARY KEY, "
        + "msg VARCHAR(110)) Engine=InnoDB", ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
    stmt.executeUpdate();
    stmt.close();
    conn.commit();
    conn.close();
  }

  /**
   * Actually run the export of the generated data to the user-created table.
   */
  public void runExport(String connectStr, String username) throws Exception {
    SqoopOptions options = new SqoopOptions(getConf());
    options.setConnectString(connectStr);
    options.setTableName("ExportStressTestTable");
    options.setUsername(username);
    options.setExportDir("ExportStressTest");
    options.setNumMappers(4);
    options.setLinesTerminatedBy('\n');
    options.setFieldsTerminatedBy(',');
    options.setExplicitOutputDelims(true);

    SqoopTool exportTool = new ExportTool();
    Sqoop sqoop = new Sqoop(exportTool, getConf(), options);
    int ret = Sqoop.runSqoop(sqoop, new String[0]);
    if (0 != ret) {
      throw new Exception("Error doing export; ret=" + ret);
    }
  }

  @Override
  public int run(String [] args) {
    String connectStr = args[0];
    String username = args[1];

    try {
      createData();
      createTable(connectStr, username);
      runExport(connectStr, username);
    } catch (Exception e) {
      System.err.println("Error: " + StringUtils.stringifyException(e));
      return 1;
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    ExportStressTest test = new ExportStressTest();
    int ret = ToolRunner.run(test, args);
    System.exit(ret);
  }
}
