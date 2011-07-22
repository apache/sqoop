/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

import com.cloudera.sqoop.testutil.ExportJobTestCase;

import org.junit.Before;

/**
 * Test that we can update a copy of data in the database,
 * based on newer data in HDFS.
 */
public class TestExportUpdate extends ExportJobTestCase {

  @Before
  public void setUp() {
    // start the server
    super.setUp();

    if (useHsqldbTestServer()) {
      // throw away any existing data that might be in the database.
      try {
        this.getTestServer().dropExistingSchema();
      } catch (SQLException sqlE) {
        fail(sqlE.toString());
      }
    }
  }
  
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

    FileSystem fs = FileSystem.getLocal(new Configuration());

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

}
