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
package org.apache.sqoop.manager.netezza;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.manager.DirectNetezzaManager;
import org.apache.sqoop.manager.NetezzaManager;
import org.junit.Before;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.TestExport;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;

/**
 * Test the Netezza implementation.
 *
 * This uses JDBC to export data from an Netezza database into HDFS. See
 * DirectNetezzaExportManualTest for external table methods.
 *
 * Since this requires an Netezza Server installation, this class is named in
 * such a way that Sqoop's default QA process does not run it. You need to run
 * this manually with -Dtestcase=NetezzaExportManualTest.
 *
 */
public class NetezzaExportManualTest extends TestExport {
  public static final Log LOG = LogFactory.getLog(NetezzaExportManualTest.class
    .getName());
  static final String TABLE_PREFIX = "EMPNZ_EXP_";
  // instance variables populated during setUp, used during tests
  protected NetezzaManager manager;
  protected Connection conn;

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  protected boolean isDirectMode() {
    return false;
  }

  @Override
  protected Connection getConnection() {
    return conn;
  }

  @Override
  protected String getConnectString() {
    return NetezzaTestUtils.getNZConnectString();
  }

  @Override
  protected String getTablePrefix() {
    return TABLE_PREFIX;
  }

  @Override
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName;
  }

  /**
   * Create the table definition to export to, removing any prior table. By
   * specifying ColumnGenerator arguments, you can add extra columns to the
   * table of arbitrary type.
   */
  @Override
  public void createTable(ColumnGenerator... extraColumns)
    throws SQLException {
    NetezzaTestUtils.createTableNZ(conn, getTableName(), extraColumns);
  }

  /**
   * Creates the staging table.
   *
   * @param extraColumns
   *          extra columns that go in the staging table
   * @throws SQLException
   *           if an error occurs during export
   */
  @Override
  public void createStagingTable(ColumnGenerator... extraColumns)
    throws SQLException {
    NetezzaTestUtils.createTableNZ(conn, getStagingTableName(), extraColumns);
  }

  @Before
  public void setUp() {
    super.setUp();
    SqoopOptions options = new SqoopOptions(
      NetezzaTestUtils.getNZConnectString(), getTableName());
    options.setUsername(NetezzaTestUtils.getNZUser());
    options.setPassword(NetezzaTestUtils.getNZPassword());
    if (isDirectMode()) {
      this.manager = new DirectNetezzaManager(options);
    } else {
      this.manager = new NetezzaManager(options);
    }

    try {
      this.conn = manager.getConnection();
      this.conn.setAutoCommit(false);
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    }
  }

  @Override
  protected String[] getArgv(boolean includeHadoopFlags, int rowsPerStatement,
    int statementsPerTx, String... additionalArgv) {

    String[] argV = super.getArgv(includeHadoopFlags,
      rowsPerStatement, statementsPerTx);
    String[] subArgV = newStrArray(argV,
      "--username", NetezzaTestUtils.getNZUser(), "--password",
      NetezzaTestUtils.getNZPassword());
    String[] newArgV = new String[subArgV.length + additionalArgv.length];
    int i = 0;
    for (String s : subArgV) {
      newArgV[i++] = s;
    }
    for (String s : additionalArgv) {
      newArgV[i++] = s;
    }
    return newArgV;
  }

  @Override
  protected String[] getCodeGenArgv(String... extraArgs) {
    String[] moreArgs;

    moreArgs = new String[extraArgs.length + 4];

    int i = 0;
    for (i = 0; i < extraArgs.length; i++) {
      moreArgs[i] = extraArgs[i];
    }

    // Add username argument for netezza.
    moreArgs[i++] = "--username";
    moreArgs[i++] = NetezzaTestUtils.getNZUser();
    moreArgs[i++] = "--password";
    moreArgs[i++] = NetezzaTestUtils.getNZPassword();

    return super.getCodeGenArgv(moreArgs);
  }

  protected void createExportFile(ColumnGenerator... extraCols)
    throws IOException {
    String ext = ".txt";

    Path tablePath = getTablePath();
    Path filePath = new Path(tablePath, "part0" + ext);

    Configuration conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(tablePath);
    OutputStream os = fs.create(filePath);

    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
    for (int i = 0; i < 3; i++) {
      String line = getRecordLine(i, extraCols);
      w.write(line);
      LOG.debug("Create Export file - Writing line : " + line);
    }
    w.close();
    os.close();
  }

  protected class NullColumnGenerator implements ColumnGenerator {
    public String getExportText(int rowNum) {
      return "\\N";
    }

    public String getVerifyText(int rowNum) {
      return null;
    }

    public String getType() {
      return "INTEGER";
    }
  }
}
