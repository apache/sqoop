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
public class HBaseImportAddRowKeyTest extends HBaseTestCase {

  @Test
  public void testAddRowKey() throws IOException {
    String[] types = { "INT", "INT" };
    String[] vals = { "0", "1" };
    createTableWithColTypes(types, vals);

    String[] otherArg = getArgv(true, "addRowKeyT", "addRowKeyF", true, null);
    String[] argv = new String[otherArg.length + 2];
    argv[0] = "-D";
    argv[1] = "sqoop.hbase.add.row.key=true";
    System.arraycopy(otherArg, 0, argv, 2, otherArg.length);

    runImport(argv);

    // Row key should have been added
    verifyHBaseCell("addRowKeyT", "0", "addRowKeyF", getColName(0), "0");
    verifyHBaseCell("addRowKeyT", "0", "addRowKeyF", getColName(1), "1");
  }

  @Test
  public void testAddRowKeyDefault() throws IOException {
    String[] types = { "INT", "INT" };
    String[] vals = { "0", "1" };
    createTableWithColTypes(types, vals);

    String[] argv = getArgv(true, "addRowKeyDfT", "addRowKeyDfF", true, null);

    runImport(argv);

    // Row key should not be added by default
    verifyHBaseCell("addRowKeyDfT", "0", "addRowKeyDfF", getColName(0), null);
    verifyHBaseCell("addRowKeyDfT", "0", "addRowKeyDfF", getColName(1), "1");
  }

  @Test
  public void testAddCompositeKey() throws IOException {
    String[] types = { "INT", "INT" };
    String[] vals = { "0", "1" };
    createTableWithColTypes(types, vals);

    String[] otherArg = getArgv(true, "addRowKeyT", "addRowKeyF", true, null);
    String[] argv = new String[otherArg.length + 4];
    argv[0]="-D";
    argv[1]="sqoop.hbase.add.row.key=true";
    System.arraycopy(otherArg, 0, argv, 2, otherArg.length);
    argv[argv.length - 2] = "--hbase-row-key";
    argv[argv.length - 1] = getColName(0)+","+getColName(1);

    runImport(argv);

    // Row key should have been added
    verifyHBaseCell("addRowKeyT", "0_1", "addRowKeyF", getColName(0), "0");
    verifyHBaseCell("addRowKeyT", "0_1", "addRowKeyF", getColName(1), "1");
  }

}
