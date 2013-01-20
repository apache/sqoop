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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ExportJobTestCase;

/**
 * Test that we can update a copy of data in the database,
 * based on newer data in HDFS.
 */
public class TestExportUpdate extends ExportJobTestCase {

  @Override
  protected String getTablePrefix() {
    return "UPDATE_TABLE_";
  }

  /**
   * Create the underlying table to update.
   */
  private void populateDatabase(int numRows) throws SQLException {
    Connection conn = getConnection();

    PreparedStatement statement = conn.prepareStatement(
        "CREATE TABLE " + getTableName()
        + " (A INT NOT NULL, B VARCHAR(32), C INT)");
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
      statement = null;
    }

    try {
      for (int i = 0; i < numRows; i++) {
        statement = conn.prepareStatement("INSERT INTO " + getTableName()
            + " VALUES (" + i + ", 'foo" + i + "', " + i + ")");
        statement.executeUpdate();
        statement.close();
        statement = null;
      }
    } finally {
      if (null != statement) {
        statement.close();
      }
    }

    conn.commit();
  }

  /**
   * <p>Creates a table with three columns - A INT, B INT and C VARCHAR(32).
   * This table is populated with records in a set of three with total records
   * with the total number of unique values of A equal to the specified aMax
   * value. For each value of A, there will be three records with value of
   * B ranging from 0-2, and a corresponding value of C.</p>
   * <p>For example if <tt>aMax = 2</tt>, the table will contain the
   * following records:
   * <pre>
   *    A   |   B   |  C
   * ----------------------
   *    0   |   0   | 0foo0
   *    0   |   1   | 0foo1
   *    0   |   2   | 0foo2
   *    1   |   0   | 1foo0
   *    1   |   1   | 1foo1
   *    1   |   2   | 1foo2
   * </pre></p>
   * @param aMax the number of
   * @throws SQLException
   */
  private void createMultiKeyTable(int aMax) throws SQLException {
    Connection conn = getConnection();

    PreparedStatement statement = conn.prepareStatement(
        "CREATE TABLE " + getTableName()
        + " (A INT NOT NULL, B INT NOT NULL, C VARCHAR(32))");
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
      statement = null;
    }

    try {
      for (int i = 0; i< aMax; i++) {
        for (int j = 0; j < 3; j++) {
          statement = conn.prepareStatement("INSERT INTO " + getTableName()
              + " VALUES (" + i + ", " + j + ", '"
              + i + "foo" + j + "')");
          statement.executeUpdate();
          statement.close();
          statement = null;
        }
      }
    } finally {
      if (null != statement) {
        statement.close();
      }
    }

    conn.commit();
  }

  /**
   * <p>Creates update files for multi-key update test. The total number of
   * update records will be number of files times the number of aKeysPerFile
   * times 3. Column A value will start with the specified <tt>startAtValue</tt>
   * and for each value there will be three records corresponding to Column
   * B values [0-2].</p>
   * @param numFiles number of files to create
   * @param aKeysPerFile number of records sets with different column A values
   * @param startAtValue the starting value of column A
   * @param bKeyValues the list of values for the column B
   * @throws IOException
   */
  private void createMultiKeyUpdateFiles(int numFiles, int aKeysPerFile,
      int startAtValue, int[] bKeyValues)
      throws IOException {
    Configuration conf = getConf();
    if (!isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);

    int aValue = startAtValue;
    for (int i = 0; i < numFiles; i++) {
      OutputStream os = fs.create(new Path(getTablePath(), "" + i + ".txt"));
      BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));

      for (int j = 0; j < aKeysPerFile; j++) {
        for (int k = 0; k < bKeyValues.length; k++) {
          w.write(getUpdateStringForMultiKeyRow(aValue, bKeyValues[k]));
        }
        aValue++;
      }

      w.close();
      os.close();
    }
  }

  /**
   * Generate a string of text representing an update for one row
   * of the multi-key table. The values of columns A and B are given
   * and the value of column C is generated as <em>a</em>bar<em>b</em>.
   * @param a the value of column a
   * @param b the value of column b
   */
  private String getUpdateStringForMultiKeyRow(int a, int b) {
    StringBuilder sb = new StringBuilder();
    sb.append(a).append("\t").append(b).append("\t").append(a);
    sb.append("bar").append(b).append("\n");

    return sb.toString();
  }


  /**
   * Create a set of files that will be used as the input to the update
   * process.
   * @param numFiles the number of files to generate
   * @param updatesPerFile the number of rows to create in each file
   * @param keyCol a value between 0 and 2 specifying whether 'a',
   * 'b', or 'c' ({@see populateDatabase()}) is the key column to keep
   * the same.
   * @param startOffsets is an optional list of row ids/values for a/c
   * which are the record ids at which the update files begin.
   * For instance, if numFiles=3, updatesPerFile=2, and keyCol=0 then
   * if startOffsets is {5, 10, 12}, files will be generated to update
   * rows with A=5,6; A=10,11; A=12,13.
   *
   * If startOffsets is empty or underspecified (given numFiles), then
   * subsequent files will start immediately after the previous file.
   */
  private void createUpdateFiles(int numFiles, int updatesPerFile,
      int keyCol, int... startOffsets) throws IOException {
    Configuration conf = getConf();
    if (!isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    FileSystem fs = FileSystem.get(conf);

    int rowId = 0;
    for (int i = 0; i < numFiles; i++) {
      OutputStream os = fs.create(new Path(getTablePath(), "" + i + ".txt"));
      BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));

      if (null != startOffsets && startOffsets.length > i) {
        // If a start offset has been specified for this file, go there.
        // Otherwise, just carry over from the previous file iteration.
        rowId = startOffsets[i];
      }

      for (int j = 0; j < updatesPerFile; j++) {
        w.write(getUpdateStringForRow(keyCol, rowId++));
      }

      w.close();
      os.close();
    }
  }

  /**
   * Generate a string of text representing an update for one row
   * of the database. keyCol is a value in [0, 2] representing which
   * column is kept fixed. rowId specifies the row being updated.
   */
  private String getUpdateStringForRow(int keyCol, int rowId) {
    StringBuilder sb = new StringBuilder();

    int [] rowInts = new int[3]; // There are 3 columns in the table.
    for (int i = 0; i < 3; i++) {
      if (keyCol == i) {
        // Keep this column fixed.
        rowInts[i] = rowId;
      } else {
        // Update the int in this column.
        rowInts[i] = rowId * 2;
      }
    }

    sb.append(rowInts[0]);
    sb.append("\tfoo");
    sb.append(rowInts[1]);
    sb.append("\t");
    sb.append(rowInts[2]);
    sb.append("\n");

    return sb.toString();
  }

  /**
   * Verifies the number of rows in the table.
   */
  private void verifyRowCount(int expectedCount) throws SQLException {
    String query = "SELECT COUNT(*) FROM " + getTableName();
    PreparedStatement statement = null;
    ResultSet rs = null;

    try {
      Connection conn = getConnection();
      statement = conn.prepareStatement(query);
      rs = statement.executeQuery();

      boolean success = rs.next();
      assertTrue("Expected at least one result", success);

      int trueCount = rs.getInt(1);
      assertEquals(expectedCount, trueCount);

      // This query should have returned exactly one row.
      success = rs.next();
      assertFalse("Expected no more than one output record", success);
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqle) {
          LOG.error("Error closing result set: "
              + StringUtils.stringifyException(sqle));
        }
      }

      if (null != statement) {
        statement.close();
      }
    }
  }

  private void verifyMultiKeyRow(String[] keyColumnNames, int[] keyValues,
      Object ...expectedVals) throws SQLException {
    StringBuilder querySb = new StringBuilder("SELECT A, B, C FROM ");
    querySb.append(getTableName()).append(" WHERE ");
    boolean first = true;
    for (int i = 0; i< keyColumnNames.length; i++) {
      if (first) {
        first = false;
      } else {
        querySb.append(" AND ");
      }
      querySb.append(keyColumnNames[i]).append(" = ");
      querySb.append(keyValues[i]);
    }

    String query = querySb.toString();
    PreparedStatement statement = null;
    ResultSet rs = null;

    try {
      Connection conn = getConnection();
      statement = conn.prepareStatement(query);
      rs = statement.executeQuery();

      boolean success = rs.next();
      assertTrue("Expected at least one output record", success);

      // Assert that all three columns have the correct values.
      for (int i = 0; i < expectedVals.length; i++) {
        String expected = expectedVals[i].toString();
        String result = rs.getString(i + 1);
        assertEquals("Invalid response for column " + i + "; got " + result
            + " when expected " + expected, expected, result);
      }

      // This query should have returned exactly one row.
      success = rs.next();
      assertFalse("Expected no more than one output record", success);
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqle) {
          LOG.error("Error closing result set: "
              + StringUtils.stringifyException(sqle));
        }
      }

      if (null != statement) {
        statement.close();
      }
    }
  }

  /**
   * Verify that a particular row has the expected values.
   */
  private void verifyRow(String keyColName, String keyVal,
      String... expectedVals) throws SQLException {
    String query = "SELECT A, B, C FROM " + getTableName() + " WHERE "
        + keyColName + " = " + keyVal;
    PreparedStatement statement = null;
    ResultSet rs = null;

    try {
      Connection conn = getConnection();
      statement = conn.prepareStatement(query);
      rs = statement.executeQuery();

      boolean success = rs.next();
      assertTrue("Expected at least one output record", success);

      // Assert that all three columns have the correct values.
      for (int i = 0; i < expectedVals.length; i++) {
        String expected = expectedVals[i];
        String result = rs.getString(i + 1);
        assertEquals("Invalid response for column " + i + "; got " + result
            + " when expected " + expected, expected, result);
      }

      // This query should have returned exactly one row.
      success = rs.next();
      assertFalse("Expected no more than one output record", success);
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqle) {
          LOG.error("Error closing result set: "
              + StringUtils.stringifyException(sqle));
        }
      }

      if (null != statement) {
        statement.close();
      }
    }
  }

  private void runUpdate(int numMappers, String updateCol) throws IOException {
    runExport(getArgv(true, 2, 2, "-m", "" + numMappers,
        "--update-key", updateCol));
  }

  public void testBasicUpdate() throws Exception {
    // Test that we can do a single-task single-file update.
    // This updates the entire database.

    populateDatabase(10);
    createUpdateFiles(1, 10, 0, 0);
    runUpdate(1, "A");
    verifyRowCount(10);
    // Check a few rows...
    verifyRow("A", "0", "0", "foo0", "0");
    verifyRow("A", "1", "1", "foo2", "2");
    verifyRow("A", "9", "9", "foo18", "18");
  }

  /**
   * Creates a table with two columns that together act as unique keys
   * and then modifies a subset of the rows via update.
   * @throws Exception
   */
  public void testMultiKeyUpdate() throws Exception {
    createMultiKeyTable(3);

    createMultiKeyUpdateFiles(1, 1, 1, new int[] {0, 1, 3});

    runExport(getArgv(true, 2, 2, "-m", "1",
        "--update-key", "A,B"));
    verifyRowCount(9);
    // Check a few rows...
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 0 }, 0, 0, "0foo0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 1 }, 0, 1, "0foo1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 2 }, 0, 2, "0foo2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 0 }, 1, 0, "1bar0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 1 }, 1, 1, "1bar1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 2 }, 1, 2, "1foo2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 0 }, 2, 0, "2foo0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 1 }, 2, 1, "2foo1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 2 }, 2, 2, "2foo2");

  }

  /**
   * Creates a table with two columns that together act as unique keys
   * and then modifies a subset of the rows via update.
   * @throws Exception
   */
  public void testMultiKeyUpdateMultipleFilesNoUpdate() throws Exception {
    createMultiKeyTable(4);

    createMultiKeyUpdateFiles(2, 1, 1, new int[] {3, 4, 5});

    runExport(getArgv(true, 2, 2, "-m", "1",
        "--update-key", "A,B"));
    verifyRowCount(12);
    // Check a few rows...
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 0 }, 0, 0, "0foo0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 1 }, 0, 1, "0foo1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 2 }, 0, 2, "0foo2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 0 }, 1, 0, "1foo0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 1 }, 1, 1, "1foo1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 2 }, 1, 2, "1foo2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 0 }, 2, 0, "2foo0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 1 }, 2, 1, "2foo1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 2 }, 2, 2, "2foo2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 3, 0 }, 3, 0, "3foo0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 3, 1 }, 3, 1, "3foo1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 3, 2 }, 3, 2, "3foo2");
  }

  /**
   * Creates a table with two columns that together act as unique keys
   * and then modifies a subset of the rows via update.
   * @throws Exception
   */
  public void testMultiKeyUpdateMultipleFilesFullUpdate() throws Exception {
    createMultiKeyTable(4);

    createMultiKeyUpdateFiles(2, 2, 0, new int[] {0, 1, 2});

    runExport(getArgv(true, 2, 2, "-m", "1",
        "--update-key", "A,B"));
    verifyRowCount(12);
    // Check a few rows...
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 0 }, 0, 0, "0bar0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 1 }, 0, 1, "0bar1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 0, 2 }, 0, 2, "0bar2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 0 }, 1, 0, "1bar0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 1 }, 1, 1, "1bar1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 1, 2 }, 1, 2, "1bar2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 0 }, 2, 0, "2bar0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 1 }, 2, 1, "2bar1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 2, 2 }, 2, 2, "2bar2");

    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 3, 0 }, 3, 0, "3bar0");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 3, 1 }, 3, 1, "3bar1");
    verifyMultiKeyRow(new String[] { "A", "B"},
        new int[] { 3, 2 }, 3, 2, "3bar2");
  }


  public void testEmptyTable() throws Exception {
    // Test that an empty table will "accept" updates that modify
    // no rows; no new data is injected into the database.
    populateDatabase(0);
    createUpdateFiles(1, 10, 0, 0);
    runUpdate(1, "A");
    verifyRowCount(0);
  }

  public void testEmptyFiles() throws Exception {
    // An empty input file results in no changes to a db table.
    populateDatabase(10);
    createUpdateFiles(1, 0, 0);
    runUpdate(1, "A");
    verifyRowCount(10);
    // Check that a few rows have not changed at all.
    verifyRow("A", "0", "0", "foo0", "0");
    verifyRow("A", "1", "1", "foo1", "1");
    verifyRow("A", "9", "9", "foo9", "9");
  }

  public void testStringCol() throws Exception {
    // Test that we can do modifications based on the string "B" column.
    populateDatabase(10);
    createUpdateFiles(1, 10, 1);
    runUpdate(1, "B");
    verifyRowCount(10);
    verifyRow("B", "'foo0'", "0", "foo0", "0");
    verifyRow("B", "'foo1'", "2", "foo1", "2");
    verifyRow("B", "'foo9'", "18", "foo9", "18");
  }

  public void testLastCol() throws Exception {
    // Test that we can do modifications based on the third int column.
    populateDatabase(10);
    createUpdateFiles(1, 10, 2);
    runUpdate(1, "C");
    verifyRowCount(10);
    verifyRow("C", "0", "0", "foo0", "0");
    verifyRow("C", "1", "2", "foo2", "1");
    verifyRow("C", "9", "18", "foo18", "9");
  }

  public void testMultiMaps() throws Exception {
    // Test that we can handle multiple map tasks.
    populateDatabase(20);
    createUpdateFiles(2, 10, 0);
    runUpdate(1, "A");
    verifyRowCount(20);
    verifyRow("A", "0", "0", "foo0", "0");
    verifyRow("A", "1", "1", "foo2", "2");
    verifyRow("A", "9", "9", "foo18", "18");
    verifyRow("A", "10", "10", "foo20", "20");
    verifyRow("A", "15", "15", "foo30", "30");
    verifyRow("A", "19", "19", "foo38", "38");
  }

  public void testSubsetUpdate() throws Exception {
    // Update only a few rows in the middle of the table.
    populateDatabase(10);
    createUpdateFiles(1, 5, 0, 3); // only rows A=3..7 change.
    runUpdate(1, "A");
    verifyRowCount(10);

    // Verify these rows are unchanged.
    verifyRow("A", "0", "0", "foo0", "0");
    verifyRow("A", "2", "2", "foo2", "2");
    verifyRow("A", "8", "8", "foo8", "8");
    verifyRow("A", "9", "9", "foo9", "9");

    // Verify these rows have been updated.
    verifyRow("A", "3", "3", "foo6", "6");
    verifyRow("A", "5", "5", "foo10", "10");
    verifyRow("A", "7", "7", "foo14", "14");
  }

  public void testSubsetUpdate2() throws Exception {
    // Update only some of the rows in the db. Also include some
    // updates that do not affect actual rows in the table.
    // These should just be ignored.

    populateDatabase(10);
    // Create two files that update four rows each.
    // File0 updates A=-2..1 (-2 and -1 don't exist).
    // File1 updates A=8..11 (10 and 11 don't exist).
    createUpdateFiles(2, 4, 0, -2, 8);
    runUpdate(2, "A");
    verifyRowCount(10);

    // Verify these rows are unchanged.
    verifyRow("A", "4", "4", "foo4", "4");
    verifyRow("A", "7", "7", "foo7", "7");

    // Verify these updates succeeded.
    verifyRow("A", "1", "1", "foo2", "2");
    verifyRow("A", "8", "8", "foo16", "16");
    verifyRow("A", "9", "9", "foo18", "18");
  }

  /**
   * Test updating only subset of the columns.
   *
   * @throws Exception
   */
  public void testUpdateColumnSubset() throws Exception {
    populateDatabase(4);
    createUpdateFiles(1, 3, 0);

    runExport(getArgv(true, 2, 2, "-m", "1",
      "--update-key", "A", "--columns", "A,B"));

    verifyRowCount(4);

    // First column should not have any changes (even though it was updated)
    verifyRow("A", "0", "0", "foo0", "0");

    // Second column have updated column B, but C should be left untouched
    verifyRow("A", "1", "1", "foo2", "1");

    // Third column have updated column B, but C should be left untouched
    verifyRow("A", "2", "2", "foo4", "2");

    // Last columns should be completely untouched
    verifyRow("A", "3", "3", "foo3", "3");
  }

  /**
   * Parameter --columns must be superset of --update-key in order for
   * CompilationManager and other parts of the framework work correctly.
   *
   * @throws Exception
   */
  public void testUpdateColumnNotInColumns() throws Exception {
    populateDatabase(1);
    try {
      runExport(getArgv(true, 2, 2, "-m", "1",
        "--update-key", "A", "--columns", "B"));
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue(true);
    }
  }

}
