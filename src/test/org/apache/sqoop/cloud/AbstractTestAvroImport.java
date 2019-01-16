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
import org.apache.sqoop.testutil.AvroTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public abstract class AbstractTestAvroImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestAvroImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected AbstractTestAvroImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testImportAsAvroDataFileWithoutDeleteTargetDirOptionWhenTargetDirDoesNotExist() throws IOException {
    String[] args = getArgsWithAsAvroDataFileOption();
    runImport(args);
    AvroTestUtils.verify(getDataSet().getExpectedAvroOutput(), fileSystemRule.getCloudFileSystem().getConf(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsAvroDataFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgsWithAsAvroDataFileOption();
    runImport(args);

    args = getArgsWithAsAvroDataFileAndDeleteTargetDirOption();
    runImport(args);
    AvroTestUtils.verify(getDataSet().getExpectedAvroOutput(), fileSystemRule.getCloudFileSystem().getConf(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsAvroDataFileWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgsWithAsAvroDataFileOption();
    runImport(args);

    thrown.expect(IOException.class);
    runImport(args);
  }

  private String[] getArgsWithAsAvroDataFileOption() {
    return getArgsForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-avrodatafile");
  }

  private String[] getArgsWithAsAvroDataFileAndDeleteTargetDirOption() {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-avrodatafile");
    builder.withOption("delete-target-dir");
    return builder.build();
  }
}
