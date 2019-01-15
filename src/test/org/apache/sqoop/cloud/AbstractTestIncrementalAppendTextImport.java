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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public abstract class AbstractTestIncrementalAppendTextImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestIncrementalAppendTextImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected AbstractTestIncrementalAppendTextImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testIncrementalAppendAsTextFileWhenNoNewRowIsImported() throws IOException {
    String[] args = getArgs(false);
    runImport(args);

    args = getIncrementalAppendArgs(false);
    runImport(args);

    failIfOutputFilePathContainingPatternExists(fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), MAP_OUTPUT_FILE_00001);
  }

  @Test
  public void testIncrementalAppendAsTextFile() throws IOException {
    String[] args = getArgs(false);
    runImport(args);

    insertInputDataIntoTable(getDataSet().getExtraInputData());

    args = getIncrementalAppendArgs(false);
    runImport(args);

    TextFileTestUtils.verify(getDataSet().getExpectedExtraTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), MAP_OUTPUT_FILE_00001);
  }

  @Test
  public void testIncrementalAppendAsTextFileWithMapreduceOutputBasenameProperty() throws IOException {
    String[] args = getArgs(true);
    runImport(args);

    insertInputDataIntoTable(getDataSet().getExtraInputData());

    args = getIncrementalAppendArgs(true);
    runImport(args);

    TextFileTestUtils.verify(getDataSet().getExpectedExtraTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), CUSTOM_MAP_OUTPUT_FILE_00001);
  }

  private String[] getArgs(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }

  private String[] getIncrementalAppendArgs(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    builder = addIncrementalAppendImportArgs(builder, fileSystemRule.getTemporaryRootDirPath().toString());
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }
}
