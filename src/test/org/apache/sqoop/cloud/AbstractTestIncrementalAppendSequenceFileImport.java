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
import org.apache.sqoop.testutil.SequenceFileTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public abstract class AbstractTestIncrementalAppendSequenceFileImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestIncrementalAppendSequenceFileImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected AbstractTestIncrementalAppendSequenceFileImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testIncrementalAppendAsSequenceFileWhenNoNewRowIsImported() throws Exception {
    String[] args = getArgsWithAsSequenceFileOption(false);
    runImport(args);

    args = getIncrementalAppendArgsWithAsSequenceFileOption(false);
    runImport(args);

    failIfOutputFilePathContainingPatternExists(fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), MAP_OUTPUT_FILE_00001);
  }

  @Test
  public void testIncrementalAppendAsSequenceFile() throws Exception {
    String[] args = getArgsWithAsSequenceFileOption(false);
    runImport(args);

    insertInputDataIntoTable(getDataSet().getExtraInputData());

    args = getIncrementalAppendArgsWithAsSequenceFileOption(false);
    runImport(args);

    SequenceFileTestUtils.verify(this, getDataSet().getExpectedExtraSequenceFileOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), MAP_OUTPUT_FILE_00001);
  }

  @Test
  public void testIncrementalAppendAsSequenceFileWithMapreduceOutputBasenameProperty() throws Exception {
    String[] args = getArgsWithAsSequenceFileOption(true);
    runImport(args);

    insertInputDataIntoTable(getDataSet().getExtraInputData());

    args = getIncrementalAppendArgsWithAsSequenceFileOption(true);
    runImport(args);

    SequenceFileTestUtils.verify(this, getDataSet().getExpectedExtraSequenceFileOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), CUSTOM_MAP_OUTPUT_FILE_00001);
  }

  private String[] getArgsWithAsSequenceFileOption(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-sequencefile");
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }

  private String[] getIncrementalAppendArgsWithAsSequenceFileOption(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-sequencefile");
    builder = addIncrementalAppendImportArgs(builder, fileSystemRule.getTemporaryRootDirPath().toString());
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }
}
