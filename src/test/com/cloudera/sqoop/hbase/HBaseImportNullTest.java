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

package com.cloudera.sqoop.hbase;

import java.io.IOException;

import org.junit.Test;

/**
 *
 */
public class HBaseImportNullTest extends HBaseTestCase {

  @Test
  public void testNullRow() throws IOException {
    String [] argv = getArgv(true, "nullRowT", "nullRowF", true, null);
    String [] types = { "INT", "INT" };
    String [] vals = { "0", "null" };
    createTableWithColTypes(types, vals);
    runImport(argv);

    // This cell should not be placed in the results..
    verifyHBaseCell("nullRowT", "0", "nullRowF", getColName(1), null);

    int rowCount = countHBaseTable("nullRowT", "nullRowF");
    assertEquals(0, rowCount);
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
