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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.TextFileTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public abstract class AbstractTestIncrementalMergeTextImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestIncrementalMergeTextImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Override
  @Before
  public void setUp() {
    super.setUp();
    createTestTableFromInitialInputDataForMerge();
  }

  protected AbstractTestIncrementalMergeTextImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testIncrementalMergeAsTextFileWhenNoNewRowIsImported() throws Exception {
    String[] args = getArgs(false);
    runImport(args);

    clearTable(getTableName());

    args = getIncrementalMergeArgs(false);
    runImport(args);

    TextFileTestUtils.verify(getDataSet().getExpectedTextOutputBeforeMerge(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), REDUCE_OUTPUT_FILE_00000);
  }

  @Test
  public void testIncrementalMergeAsTextFile() throws Exception {
    String[] args = getArgs(false);
    runImport(args);

    clearTable(getTableName());

    insertInputDataIntoTableForMerge(getDataSet().getNewInputDataForMerge());

    args = getIncrementalMergeArgs(false);
    runImport(args);

    TextFileTestUtils.verify(getDataSet().getExpectedTextOutputAfterMerge(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), REDUCE_OUTPUT_FILE_00000);
  }

  @Test
  public void testIncrementalMergeAsTextFileWithMapreduceOutputBasenameProperty() throws Exception {
    String[] args = getArgs(true);
    runImport(args);

    clearTable(getTableName());

    insertInputDataIntoTableForMerge(getDataSet().getNewInputDataForMerge());

    args = getIncrementalMergeArgs(true);
    runImport(args);

    TextFileTestUtils.verify(getDataSet().getExpectedTextOutputAfterMerge(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), CUSTOM_REDUCE_OUTPUT_FILE_00000);
  }

  private String[] getArgs(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }

  private String[] getIncrementalMergeArgs(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    builder = addIncrementalMergeImportArgs(builder, fileSystemRule.getTemporaryRootDirPath().toString());
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }
}
