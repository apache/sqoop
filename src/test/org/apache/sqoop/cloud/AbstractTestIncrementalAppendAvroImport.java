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
import org.apache.sqoop.testutil.AvroTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public abstract class AbstractTestIncrementalAppendAvroImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestIncrementalAppendAvroImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected AbstractTestIncrementalAppendAvroImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testIncrementalAppendAsAvroDataFileWhenNoNewRowIsImported() throws IOException {
    String[] args = getArgsWithAsAvroDataFileOption(false);
    runImport(args);

    args = getIncrementalAppendArgsWithAsAvroDataFileOption(false);
    runImport(args);

    failIfOutputFilePathContainingPatternExists(fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath(), MAP_OUTPUT_FILE_00001);
  }

  @Test
  public void testIncrementalAppendAsAvroDataFile() throws IOException {
    String[] args = getArgsWithAsAvroDataFileOption(false);
    runImport(args);

    insertInputDataIntoTable(getDataSet().getExtraInputData());

    args = getIncrementalAppendArgsWithAsAvroDataFileOption(false);
    runImport(args);

    AvroTestUtils.verify(getDataSet().getExpectedExtraAvroOutput(), fileSystemRule.getCloudFileSystem().getConf(), fileSystemRule.getTargetDirPath(), MAP_OUTPUT_FILE_00001);
  }

  @Test
  public void testIncrementalAppendAsAvroDataFileWithMapreduceOutputBasenameProperty() throws IOException {
    String[] args = getArgsWithAsAvroDataFileOption(true);
    runImport(args);

    insertInputDataIntoTable(getDataSet().getExtraInputData());

    args = getIncrementalAppendArgsWithAsAvroDataFileOption(true);
    runImport(args);

    AvroTestUtils.verify(getDataSet().getExpectedExtraAvroOutput(), fileSystemRule.getCloudFileSystem().getConf(), fileSystemRule.getTargetDirPath(), CUSTOM_MAP_OUTPUT_FILE_00001);
  }

  private String[] getArgsWithAsAvroDataFileOption(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-avrodatafile");
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }

  private String[] getIncrementalAppendArgsWithAsAvroDataFileOption(boolean withMapreduceOutputBasenameProperty) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-avrodatafile");
    builder = addIncrementalAppendImportArgs(builder, fileSystemRule.getTemporaryRootDirPath().toString());
    if (withMapreduceOutputBasenameProperty) {
      builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, MAPREDUCE_OUTPUT_BASENAME);
    }
    return builder.build();
  }
}
