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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.manager.DirectNetezzaManager;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;

/**
 * Test the DirectNetezzaManager implementation's exportJob() functionality.
 */
public class DirectNetezzaExportManualTest extends NetezzaExportManualTest {

  public static final Log LOG = LogFactory
      .getLog(DirectNetezzaExportManualTest.class.getName());

  static final String TABLE_PREFIX = "EMPNZ_D_EXP";

  @Override
  protected String getTablePrefix() {
    return TABLE_PREFIX;
  }

  @Override
  protected boolean isDirectMode() {
    return true;
  }

  @Override
  protected String[] getCodeGenArgv(String... extraArgs) {

    String[] moreArgs = new String[extraArgs.length + 4];
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

  @Override
  protected String[] getArgv(boolean includeHadoopFlags, int rowsPerStatement,
      int statementsPerTx, String... additionalArgv) {

    String[] argV = super.getArgv(includeHadoopFlags,
        rowsPerStatement, statementsPerTx);
    String[] subArgV = newStrArray(argV, "--direct",
      "--username",  NetezzaTestUtils.getNZUser(), "--password",
      NetezzaTestUtils.getNZPassword());
    String[] newArgV = new String[subArgV.length + additionalArgv.length];
    int i = 0;
    for (String s : subArgV) {
      newArgV[i++] = s;
    }
    for (String s: additionalArgv) {
      newArgV[i++] = s;
    }
    return newArgV;
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
   * @param extraColumns extra columns that go in the staging table
   * @throws SQLException if an error occurs during export
   */
  @Override
  public void createStagingTable(ColumnGenerator... extraColumns)
    throws SQLException {
    NetezzaTestUtils.createTableNZ(conn, getStagingTableName(), extraColumns);
  }



  protected void runNetezzaTest(String tableName, String[] argv,
    ColumnGenerator...extraCols) throws IOException {
    SqoopOptions options = new SqoopOptions(
        NetezzaTestUtils.getNZConnectString(), getTableName());
    options.setUsername(NetezzaTestUtils.getNZUser());
    options.setPassword(NetezzaTestUtils.getNZPassword());

    LOG.info("Running export with argv : " + Arrays.toString(argv));
    manager = new DirectNetezzaManager(options);

    try {
      createTable(extraCols);
      LOG.info("Created table " + tableName);
      createExportFile(extraCols);
      // run the export and verify that the results are good.
      runExport(argv);
      verifyExport(3, conn);
      if (extraCols.length > 0) {
        assertColMinAndMax(forIdx(0), extraCols[0]);
      }
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when accessing target table. " + sqlE);
    }
  }

  /**
   * Test an authenticated export using netezza external table import.
   */
  @Test
  public void testSimpleExport() throws IOException, SQLException {
    String[] argv = getArgv(true, 10, 10);
    runNetezzaTest(getTableName(), argv);
  }

  @Test
  public void testValidExtraArgs() throws Exception {

    String [] extraArgs = {
        "--",
        "--log-dir", "/tmp",
        "--max-errors", "2",
        "--trunc-string",
        "--ctrl-chars"
     };
    String[] argv = getArgv(true, 10, 10, extraArgs);
    runNetezzaTest(getTableName(), argv);
  }

  @Test
  public void testNullStringExport() throws Exception {

    String [] extraArgs = {
        "--input-null-string", "\\\\N",
        "--input-null-non-string", "\\\\N",
        "--input-escaped-by", "\\",
    };
    ColumnGenerator[] extraCols = new ColumnGenerator[] {
       new NullColumnGenerator(),
    };

    String[] argv = getArgv(true, 10, 10, extraArgs);
    runNetezzaTest(getTableName(), argv, extraCols);
  }


  public void testDifferentNullStrings() throws IOException, SQLException {
    ColumnGenerator[] extraCols = new ColumnGenerator[] {
        new NullColumnGenerator(),
    };

    String [] extraArgs = {
       "--input-null-string", "\\N",
       "--input-null-non-string", "\\M",
    };
    String[] argv = getArgv(true, 10, 10, extraArgs);
    try {
      runNetezzaTest(getTableName(), argv, extraCols);
      fail("Expected failure for different null strings");
    } catch(IOException ioe) {
      // success
    }
  }

  @Test(expected = java.io.IOException.class)
  public void testLongNullStrings() throws IOException, SQLException {
    ColumnGenerator[] extraCols = new ColumnGenerator[] {
        new NullColumnGenerator(),
    };

    String [] extraArgs = {
       "--input-null-string", "morethan4chars",
       "--input-null-non-string", "morethan4chars",
    };
    String[] argv = getArgv(true, 10, 10, extraArgs);
    try {
      runNetezzaTest(getTableName(), argv, extraCols);
      fail("Expected failure for long null strings");
    } catch(IOException ioe) {
      // success
    }
  }


  @Override
  public void testMultiMapTextExportWithStaging() throws IOException,
      SQLException {
    // disable this test as staging is not supported in direct mode
  }

  @Override
  public void testMultiTransactionWithStaging() throws IOException,
      SQLException {
    // disable this test as staging is not supported in direct mode
  }

  @Override
  public void testColumnsExport() throws IOException, SQLException {
    // disable this test as it is not supported in direct mode
  }

  @Override
  public void testSequenceFileExport() throws IOException, SQLException {
    // disable this test as it is not supported in direct mode
  }
}
