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

package org.apache.sqoop.tool;

import com.cloudera.sqoop.SqoopOptions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;

import static com.cloudera.sqoop.SqoopOptions.FileLayout.SequenceFile;
import static com.cloudera.sqoop.SqoopOptions.FileLayout.AvroDataFile;
import static com.cloudera.sqoop.SqoopOptions.FileLayout.ParquetFile;
import static com.cloudera.sqoop.SqoopOptions.FileLayout.TextFile;

@RunWith(Parameterized.class)
public class ImportToolValidateOptionsTest {

  @Parameters(name = "fileLayout = {0}, validationMessage = {1}")
  public static Iterable<? extends Object> fileLayoutAndValidationMessageParameters() {
    return Arrays.asList(new Object[] {SequenceFile, String.format("Can't run HBase import with file layout: %s", SequenceFile)},
        new Object[] {AvroDataFile, String.format("Can't run HBase import with file layout: %s", AvroDataFile)},
        new Object[] {ParquetFile, String.format("Can't run HBase import with file layout: %s", ParquetFile)});
  }

  private static final String TABLE_NAME = "testTableName";
  private static final String CONNECT_STRING = "testConnectString";
  private static final String CHECK_COLUMN_NAME = "checkColumnName";
  private static final String HBASE_TABLE_NAME = "testHBaseTableName";
  private static final String HBASE_COL_FAMILY = "testHBaseColumnFamily";
  private SqoopOptions.FileLayout fileLayout;
  private String validationMessage;

  public ImportToolValidateOptionsTest(SqoopOptions.FileLayout fileLayout, String validationMessage) {
    this.fileLayout = fileLayout;
    this.validationMessage = validationMessage;
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private ImportTool importTool;

  @Before
  public void setup() {
    importTool = new ImportTool();
    importTool.extraArguments = new String[0];
  }

  @Test
  public void testValidationFailsWithHiveImportAndIncrementalLastmodified() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.DateLastModified);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.expectMessage(BaseSqoopTool.HIVE_IMPORT_WITH_LASTMODIFIED_NOT_SUPPORTED);

    importTool.validateOptions(options);
  }

  /**
   * Note that append mode (--append) is designed to be used with HDFS import and not Hive import.
   * However this test case is added to make sure that the error message generated is correct even if --append is used.
   *
   */
  @Test
  public void testValidationFailsWithHiveImportAndAppendModeIncrementalLastmodified() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.DateLastModified);
    options.setAppendMode(true);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.expectMessage(BaseSqoopTool.HIVE_IMPORT_WITH_LASTMODIFIED_NOT_SUPPORTED);

    importTool.validateOptions(options);
  }

  @Test
  public void testValidationSucceedsWithHiveImportAndIncrementalAppendRows() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.AppendRows);

    importTool.validateOptions(options);
  }

  /**
   * Note that append mode (--append) is designed to be used with HDFS import and not Hive import.
   * However this test case is added to make sure that SQOOP-2986 does not break the already existing validation.
   *
   */
  @Test
  public void testValidationSucceedsWithHiveImportAndAppendModeAndIncrementalAppendRows() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.AppendRows);
    options.setAppendMode(true);

    importTool.validateOptions(options);
  }

  @Test
  public void testValidationFailsWithHBaseImportAndFileLayoutDifferentFromTexFile() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHBaseTable(HBASE_TABLE_NAME);
    options.setHBaseColFamily(HBASE_COL_FAMILY);
    options.setFileLayout(fileLayout);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.expectMessage(validationMessage);

    importTool.validateOptions(options);
  }

  @Test
  public void testValidationSucceedsWithHBaseImportAndAsTextFile() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHBaseTable(HBASE_TABLE_NAME);
    options.setHBaseColFamily(HBASE_COL_FAMILY);
    options.setFileLayout(TextFile);

    thrown.none();

    importTool.validateOptions(options);
  }

  private SqoopOptions buildBaseSqoopOptions() {
    SqoopOptions result = new SqoopOptions();
    result.setTableName(TABLE_NAME);
    result.setConnectString(CONNECT_STRING);
    return result;
  }

}

