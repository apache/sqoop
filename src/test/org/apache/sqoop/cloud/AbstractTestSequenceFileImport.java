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
import org.apache.sqoop.testutil.SequenceFileTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public abstract class AbstractTestSequenceFileImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestSequenceFileImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected AbstractTestSequenceFileImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testImportAsSequenceFileWithoutDeleteTargetDirOptionWhenTargetDirDoesNotExist() throws Exception {
    String[] args = getArgsWithAsSequenceFileOption();
    runImport(args);
    SequenceFileTestUtils.verify(this, getDataSet().getExpectedSequenceFileOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsSequenceFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
    String[] args = getArgsWithAsSequenceFileOption();
    runImport(args);

    args = getArgsWithAsSequenceFileAndDeleteTargetDirOption();
    runImport(args);
    SequenceFileTestUtils.verify(this, getDataSet().getExpectedSequenceFileOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsSequenceWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
    String[] args = getArgsWithAsSequenceFileOption();
    runImport(args);

    thrown.expect(IOException.class);
    runImport(args);
  }

  private String[] getArgsWithAsSequenceFileOption() {
    return getArgsForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-sequencefile");
  }

  private String[] getArgsWithAsSequenceFileAndDeleteTargetDirOption() {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-sequencefile");
    builder.withOption("delete-target-dir");
    return builder.build();
  }
}
