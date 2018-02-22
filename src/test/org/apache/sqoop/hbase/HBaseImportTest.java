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

package org.apache.sqoop.hbase;

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test imports of tables into HBase.
 */
public class HBaseImportTest extends HBaseTestCase {

  @Test
  public void testBasicUsage() throws IOException {
    // Create the HBase table in Sqoop as we run the job.
    String [] argv = getArgv(true, "BasicUsage", "BasicColFam", true, null);
    String [] types = { "INT", "INT" };
    String [] vals = { "0", "1" };
    createTableWithColTypes(types, vals);
    runImport(argv);
    verifyHBaseCell("BasicUsage", "0", "BasicColFam", getColName(1), "1");
  }

  @Test
  public void testMissingTableFails() throws IOException {
    // Test that if the table doesn't exist, we fail unless we
    // explicitly create the table.
    String [] argv = getArgv(true, "MissingTable", "MissingFam", false, null);
    String [] types = { "INT", "INT" };
    String [] vals = { "0", "1" };
    createTableWithColTypes(types, vals);
    try {
      runImport(argv);
      fail("Expected IOException");
    } catch (IOException ioe) {
      LOG.info("Got exception -- ok; we expected that job to fail.");
    }
  }

  @Test
  public void testOverwriteSucceeds() throws IOException {
    // Test that we can create a table and then import immediately
    // back on top of it without problem.
    String [] argv = getArgv(true, "OverwriteT", "OverwriteF", true, null);
    String [] types = { "INT", "INT" };
    String [] vals = { "0", "1" };
    createTableWithColTypes(types, vals);
    runImport(argv);
    verifyHBaseCell("OverwriteT", "0", "OverwriteF", getColName(1), "1");
    // Run a second time.
    runImport(argv);
    verifyHBaseCell("OverwriteT", "0", "OverwriteF", getColName(1), "1");
  }

  @Test
  public void testAppendWithTimestampSucceeds() throws IOException {
    // Test that we can create a table and then import multiple rows
    // validate for append scenario with time stamp
    String [] argv = getArgv(true, "AppendTable", "AppendColumnFamily", true, null);
    String [] types = { "INT", "INT", "INT", "DATETIME" };
    String [] vals = { "0", "1", "1", "'2017-03-20'" };
    createTableWithColTypes(types, vals);
    runImport(argv);
    verifyHBaseCell("AppendTable", "0", "AppendColumnFamily", getColName(2), "1");
    // Run a second time.
    argv = getIncrementalArgv(true, "AppendTable", "AppendColumnFamily", true, null, true, false, "DATA_COL1", "2017-03-24 01:01:01.0", null, "ignore");
    vals = new String[] { "1", "2", "3", "'2017-06-15'" };
    insertIntoTable(types, vals);
    runImport(argv);
    verifyHBaseCell("AppendTable", "1", "AppendColumnFamily", getColName(2), "3");
  }

  @Test
  public void testAppendSucceeds() throws IOException {
	// Test that we can create a table and then import multiple rows
	// validate for append scenario with ID column(DATA_COL3)
    String [] argv = getArgv(true, "AppendTable", "AppendColumnFamily", true, null);
    String [] types = { "INT", "INT", "INT", "DATETIME" };
    String [] vals = { "0", "1", "1", "'2017-03-20'" };
    createTableWithColTypes(types, vals);
    runImport(argv);
    verifyHBaseCell("AppendTable", "0", "AppendColumnFamily", getColName(2), "1");
    // Run a second time.
    argv = getIncrementalArgv(true, "AppendTable", "AppendColumnFamily", true, null, true, true, "DATA_COL1", null, "DATA_COL3", "ignore");
    vals = new String[] { "1", "2", "3", "'2017-06-15'" };
    insertIntoTable(types, vals);
    runImport(argv);
    verifyHBaseCell("AppendTable", "1", "AppendColumnFamily", getColName(2), "3");
  }

  @Test
  public void testNullIncrementalModeIgnore() throws Exception {
    // Latest value retained with 'ignore' mode
    runInsertUpdateUpdateDeleteAndExpectValue("ignore", "2");
  }

  @Test
  public void testNullIncrementalModeDelete() throws Exception {
    // All previous values deleted with 'delete' mode
    runInsertUpdateUpdateDeleteAndExpectValue("delete", null);
  }

  /**
   * Does the following
   *  - create HBase table
   *  - insert value "1"
   *  - update value to "2"
   *  - update value to null
   *  - asserts its value equals expectedValue
   *
   * @param nullMode hbase-null-incremental-mode to use ('ignore' or 'delete')
   * @param expectedValue expected value in the end
   * @throws Exception
   */
  private void runInsertUpdateUpdateDeleteAndExpectValue(String nullMode, String expectedValue) throws Exception {
    // Create table and import with initial values
    String [] types = { "INT", "INT", "DATETIME" };
    createTableWithColTypes(types, new String[] { "0", "1", "'2017-03-20'" });
    runImport(getArgv(true, "OverwriteTable", "OverwriteColumnFamily", true, null));
    verifyHBaseCell("OverwriteTable", "0", "OverwriteColumnFamily", getColName(1), "1");

    // Run a second time after updating.
    updateTable(types, new String[] { "0", "2", "'2017-03-25'" });
    runImport(getIncrementalArgv(true, "OverwriteTable", "OverwriteColumnFamily", true, null, false, false, getColName(2), "2017-03-24 01:01:01.0", null, nullMode));
    verifyHBaseCell("OverwriteTable", "0", "OverwriteColumnFamily", getColName(1), "2");

    // Run third time after deleting (setting to null)
    updateTable(types, new String[] { "0", null, "'2017-03-28'" });
    runImport(getIncrementalArgv(true, "OverwriteTable", "OverwriteColumnFamily", true, null, false, false, getColName(2), "2017-03-26 01:01:01.0", null, nullMode));
    verifyHBaseCell("OverwriteTable", "0", "OverwriteColumnFamily", getColName(1), expectedValue);
  }


  @Test
  public void testExitFailure() throws IOException {
    String [] types = { "INT", "INT", "INT" };
    String [] vals = { "0", "42", "43" };
    createTableWithColTypes(types, vals);

    String [] argv = getArgv(true, "NoHBaseT", "NoHBaseF", true, null);
    try {
      HBaseUtil.setAlwaysNoHBaseJarMode(true);
      runImport(argv);
    } catch (IOException e)  {
      return;
    } finally {
      HBaseUtil.setAlwaysNoHBaseJarMode(false);
    }

    fail("should have gotten exception");
  }

}
