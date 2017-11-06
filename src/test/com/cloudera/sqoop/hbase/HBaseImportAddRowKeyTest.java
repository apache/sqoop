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

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.join;

@RunWith(Parameterized.class)
public class HBaseImportAddRowKeyTest extends HBaseTestCase {

  @Parameterized.Parameters(name = "bulkLoad = {0}")
  public static Iterable<? extends Object> bulkLoadParameters() {
    return Arrays.asList(new Boolean[] { false } , new Boolean[] { true } );
  }

  private String[] columnTypes;

  private String[] columnValues;

  private String hbaseTableName;

  private String hbaseColumnFamily;

  private String hbaseTmpDir;

  private String hbaseBulkLoadDir;

  private boolean bulkLoad;

  public HBaseImportAddRowKeyTest(boolean bulkLoad) {
    this.bulkLoad = bulkLoad;
  }

  @Before
  public void setUp() {
    super.setUp();
    columnTypes = new String[] { "INT", "INT" };
    columnValues = new String[] { "0", "1" };
    hbaseTableName = "addRowKeyTable";
    hbaseColumnFamily = "addRowKeyFamily";
    hbaseTmpDir = TEMP_BASE_DIR + "hbaseTmpDir";
    hbaseBulkLoadDir = TEMP_BASE_DIR + "hbaseBulkLoadDir";
    createTableWithColTypes(columnTypes, columnValues);
  }

  @Test
  public void testAddRowKey() throws IOException {
    String[] argv = getImportArguments(true, hbaseTableName, hbaseColumnFamily);

    runImport(argv);

    // Row key should have been added
    verifyHBaseCell(hbaseTableName, columnValues[0], hbaseColumnFamily, getColName(0), columnValues[0]);
    verifyHBaseCell(hbaseTableName, columnValues[0], hbaseColumnFamily, getColName(1), columnValues[1]);
  }

  @Test
  public void testAddRowKeyDefault() throws IOException {
    String[] argv = getImportArguments(false, hbaseTableName, hbaseColumnFamily);

    runImport(argv);

    // Row key should not be added by default
    verifyHBaseCell(hbaseTableName, columnValues[0], hbaseColumnFamily, getColName(0), null);
    verifyHBaseCell(hbaseTableName, columnValues[0], hbaseColumnFamily, getColName(1), columnValues[1]);
  }

  @Test
  public void testAddCompositeKey() throws IOException {
    String rowKey = getColName(0)+","+getColName(1);

    String[] argv = getImportArguments(true, hbaseTableName, hbaseColumnFamily, rowKey);

    runImport(argv);

    // Row key should have been added
    verifyHBaseCell(hbaseTableName, join(columnValues, '_'), hbaseColumnFamily, getColName(0), columnValues[0]);
    verifyHBaseCell(hbaseTableName, join(columnValues, '_'), hbaseColumnFamily, getColName(1), columnValues[1]);
  }

  private String[] getImportArguments(boolean addRowKey, String hbaseTableName, String hbaseColumnFamily) {
    return getImportArguments(addRowKey, hbaseTableName, hbaseColumnFamily, null);
  }

  private String[] getImportArguments(boolean addRowKey, String hbaseTableName, String hbaseColumnFamily, String rowKey) {
    List<String> result = new ArrayList<>();

    if (addRowKey) {
      result.add("-D");
      result.add("sqoop.hbase.add.row.key=true");
    }
    result.add("-D");
    result.add("hbase.fs.tmp.dir=" + hbaseTmpDir);

    result.addAll(asList(getArgv(true, hbaseTableName, hbaseColumnFamily, true, null)));

    if(bulkLoad) {
      result.add("--target-dir");
      result.add(hbaseBulkLoadDir);
      result.add("--hbase-bulkload");
    }

    if (!StringUtils.isBlank(rowKey)) {
      result.add("--hbase-row-key");
      result.add(rowKey);
    }

    return result.toArray(new String[result.size()]);
  }

}
