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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.TextFileTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public abstract class AbstractTestTextImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestTextImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected AbstractTestTextImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testImportWithoutDeleteTargetDirOptionWhenTargetDirDoesNotExist() throws IOException {
    String[] args = getArgs(false);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(false);
    runImport(args);

    args = getArgsWithDeleteTargetOption(false);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(false);
    runImport(args);

    thrown.expect(IOException.class);
    runImport(args);
  }

  @Test
  public void testImportAsTextFile() throws IOException {
    String[] args = getArgs(true);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsTextFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(true);
    runImport(args);

    args = getArgsWithDeleteTargetOption(true);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsTextFileWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(true);
    runImport(args);

    thrown.expect(IOException.class);
    runImport(args);
  }

  private String[] getArgs(boolean withAsTextFileOption) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    if (withAsTextFileOption) {
      builder.withOption("as-textfile");
    }
    return builder.build();
  }

  private String[] getArgsWithDeleteTargetOption(boolean withAsTextFileOption) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    builder.withOption("delete-target-dir");
    if (withAsTextFileOption) {
      builder.withOption("as-textfile");
    }
    return builder.build();
  }
}
