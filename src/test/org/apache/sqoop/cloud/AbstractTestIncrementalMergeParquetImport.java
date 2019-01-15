/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.cloud;

import static org.apache.sqoop.util.AppendUtils.MAPREDUCE_OUTPUT_BASENAME_PROPERTY;
import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.util.ParquetReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public abstract class AbstractTestIncrementalMergeParquetImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestIncrementalMergeParquetImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Override
  @Before
  public void setUp() {
    super.setUp();
    createTestTableFromInitialInputDataForMerge();
  }

  protected AbstractTestIncrementalMergeParquetImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testIncrementalMergeAsParquetFileWhenNoNewRowIsImported() throws Exception {
    String[] args = getArgsWithAsParquetFileOption(false);
    runImport(args);

    clearTable(getTableName());

    args = getIncrementalMergeArgsWithAsParquetFileOption(false);
    runImport(args);

    List<String> result = new ParquetReader(fileSystemRule.getTargetDirPath(), fileSystemRule.getCloudFileSystem().getConf()).readAllInCsvSorted();
    assertEquals(getDataSet().getExpectedParquetOutputWithTimestampColumn(), result);
  }

  @Test
  public void testIncrementalMergeAsParquetFile() throws Exception {
    String[] args = getArgsWithAsParquetFileOption(false);
    runImport(args);

    clearTable(getTableName());

    insertInputDataIntoTableForMerge(getDataSet().getNewInputDataForMerge());

    args = getIncrementalMergeArgsWithAsParquetFileOption(false);
    runImport(args);

    List<String> result = new ParquetReader(fileSystemRule.getTargetDirPath(), fileSystemRule.getCloudFileSystem().getConf()).readAllInCsvSorted();
    assertEquals(getDataSet().getExpectedParquetOutputWithTimestampColumnAfterMerge(), result);
  }

  @Test
  public void testIncrementalMergeAsParquetFileWithMapreduceOutputBasenameProperty() throws Exception {
    String[] args = getArgsWithAsParquetFileOption(true);
    runImport(args);

    clearTable(getTableName());

    insertInputDataIntoTableForMerge(getDataSet().getNewInputDataForMerge());

    args = getIncrementalMergeArgsWithAsParquetFileOption(true);
    runImport(args);

    failIfOutputFilePathContainingPatternDoesNotExists(fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), MAPREDUCE_OUTPUT_BASENAME);

    List<String> result = new ParquetReader(fileSystemRule.getTargetDirPath(), fileSystemRule.getCloudFileSystem().getConf()).readAllInCsvSorted();
    assertEquals(getDataSet().getExpectedParquetOutputWithTimestampColumnAfterMerge(), result);
  }

  private String[] getArgsWithAsParquetFileOption(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-parquetfile");
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }

  private String[] getIncrementalMergeArgsWithAsParquetFileOption(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-parquetfile");
    builder = addIncrementalMergeImportArgs(builder, fileSystemRule.getTemporaryRootDirPath().toString());
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }
}
