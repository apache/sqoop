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

package com.cloudera.sqoop.hbase;

import java.io.IOException;

import org.junit.Test;

/**
 * Test imports of tables into HBase.
 */
public class TestHBaseImport extends HBaseTestCase {

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
  public void testStrings() throws IOException {
    String [] argv = getArgv(true, "stringT", "stringF", true, null);
    String [] types = { "INT", "VARCHAR(32)" };
    String [] vals = { "0", "'abc'" };
    createTableWithColTypes(types, vals);
    runImport(argv);
    verifyHBaseCell("stringT", "0", "stringF", getColName(1), "abc");
  }

  @Test
  public void testNulls() throws IOException {
    String [] argv = getArgv(true, "nullT", "nullF", true, null);
    String [] types = { "INT", "INT", "INT" };
    String [] vals = { "0", "42", "null" };
    createTableWithColTypes(types, vals);
    runImport(argv);

    // This cell should import correctly.
    verifyHBaseCell("nullT", "0", "nullF", getColName(1), "42");

    // This cell should not be placed in the results..
    verifyHBaseCell("nullT", "0", "nullF", getColName(2), null);
  }
}
