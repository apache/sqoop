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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Test cases that import rows containing multiple columns,
 * some of which may contain null values.
 *
 * Also test loading only selected columns from the db.
 */
public class TestMultiCols extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      TestMultiCols.class.getName());

  /**
   * Do a full import verification test on a table containing one row.
   * @param types the types of the columns to insert
   * @param insertVals the SQL text to use to insert each value
   * @param validateVals the text to expect when retrieving each value from
   * the db
   * @param validateLine the text to expect as a toString() of the entire row,
   * as imported by the tool
   * @param importColumns The list of columns to import
   */
  private void verifyTypes(String [] types , String [] insertVals,
      String validateLine) {
    verifyTypes(types, insertVals, validateLine, null);
  }

  private void verifyTypes(String [] types , String [] insertVals,
      String validateLine, String [] importColumns) {

    createTableWithColTypes(types, insertVals);
    verifyImport(validateLine, importColumns);
    LOG.debug("Verified input line as " + validateLine + " -- ok!");
  }

  public void testThreeStrings() {
    String [] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)" };
    String [] insertVals = { "'foo'", "'bar'", "'baz'" };
    String validateLine = "foo,bar,baz";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testStringsWithNull1() {
    String [] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)" };
    String [] insertVals = { "'foo'", "null", "'baz'" };
    String validateLine = "foo,null,baz";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testStringsWithNull2() {
    String [] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)" };
    String [] insertVals = { "null", "'foo'", "'baz'" };
    String validateLine = "null,foo,baz";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testStringsWithNull3() {
    String [] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)" };
    String [] insertVals = { "'foo'", "'baz'", "null"};
    String validateLine = "foo,baz,null";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testThreeInts() {
    String [] types = { "INTEGER", "INTEGER", "INTEGER" };
    String [] insertVals = { "1", "2", "3" };
    String validateLine = "1,2,3";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testIntsWithNulls() {
    String [] types = { "INTEGER", "INTEGER", "INTEGER" };
    String [] insertVals = { "1", "null", "3" };
    String validateLine = "1,null,3";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testMixed1() {
    String [] types = { "INTEGER", "VARCHAR(32)", "DATE" };
    String [] insertVals = { "1", "'meep'", "'2009-12-31'" };
    String validateLine = "1,meep,2009-12-31";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testMixed2() {
    String [] types = { "INTEGER", "VARCHAR(32)", "DATE" };
    String [] insertVals = { "null", "'meep'", "'2009-12-31'" };
    String validateLine = "null,meep,2009-12-31";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testMixed3() {
    String [] types = { "INTEGER", "VARCHAR(32)", "DATE" };
    String [] insertVals = { "1", "'meep'", "null" };
    String validateLine = "1,meep,null";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testMixed4() {
    String [] types = { "NUMERIC", "INTEGER", "NUMERIC" };
    String [] insertVals = { "-42", "17", "33333333333333333333333.1714" };
    String validateLine = "-42,17,33333333333333333333333.1714";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testMixed5() {
    String [] types = { "NUMERIC", "INTEGER", "NUMERIC" };
    String [] insertVals = { "null", "17", "33333333333333333333333.0" };
    String validateLine = "null,17,33333333333333333333333.0";

    verifyTypes(types, insertVals, validateLine);
  }

  public void testMixed6() {
    String [] types = { "NUMERIC", "INTEGER", "NUMERIC" };
    String [] insertVals = { "33333333333333333333333", "17", "-42"};
    String validateLine = "33333333333333333333333,17,-42";

    verifyTypes(types, insertVals, validateLine);
  }

  //////////////////////////////////////////////////////////////////////////
  // the tests below here test the --columns parameter and ensure that
  // we can selectively import only certain columns.
  //////////////////////////////////////////////////////////////////////////

  public void testSkipFirstCol() {
    String [] types = { "NUMERIC", "INTEGER", "NUMERIC" };
    String [] insertVals = { "33333333333333333333333", "17", "-42"};
    String validateLine = "17,-42";

    String [] loadCols = {"DATA_COL1", "DATA_COL2"};

    verifyTypes(types, insertVals, validateLine, loadCols);
  }

  public void testSkipSecondCol() {
    String [] types = { "NUMERIC", "INTEGER", "NUMERIC" };
    String [] insertVals = { "33333333333333333333333", "17", "-42"};
    String validateLine = "33333333333333333333333,-42";

    String [] loadCols = {"DATA_COL0", "DATA_COL2"};

    verifyTypes(types, insertVals, validateLine, loadCols);
  }

  public void testSkipThirdCol() {
    String [] types = { "NUMERIC", "INTEGER", "NUMERIC" };
    String [] insertVals = { "33333333333333333333333", "17", "-42"};
    String validateLine = "33333333333333333333333,17";

    String [] loadCols = {"DATA_COL0", "DATA_COL1"};

    verifyTypes(types, insertVals, validateLine, loadCols);
  }

  /**
   * This tests that the columns argument can handle comma-separated column
   * names.  So this is like having:
   *   --columns "DATA_COL0,DATA_COL1,DATA_COL2"
   * as two args on a sqoop command line
   *
   * @throws IOException
   */
  public void testSingleColumnsArg() throws IOException {
    String [] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)" };
    String [] insertVals = { "'foo'", "'bar'", "'baz'" };
    String validateLine = "foo,bar,baz";
    String [] loadCols = {"DATA_COL0,DATA_COL1,DATA_COL2"};

    verifyTypes(types, insertVals, validateLine, loadCols);
  }

  /**
   * This tests that the columns argument can handle spaces between column
   * names.  So this is like having:
   *   --columns "DATA_COL0, DATA_COL1, DATA_COL2"
   * as two args on a sqoop command line
   *
   * @throws IOException
   */
  public void testColumnsWithSpaces() throws IOException {
    String [] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)" };
    String [] insertVals = { "'foo'", "'bar'", "'baz'" };
    String validateLine = "foo,bar,baz";
    String [] loadCols = {"DATA_COL0, DATA_COL1, DATA_COL2"};

    verifyTypes(types, insertVals, validateLine, loadCols);
  }
}
