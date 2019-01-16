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

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.util.ParquetReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

public abstract class AbstractTestParquetImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestParquetImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected AbstractTestParquetImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testImportAsParquetFileWithoutDeleteTargetDirOptionWhenTargetDirDoesNotExist() throws Exception {
    String[] args = getArgsWithAsParquetFileOption();
    runImport(args);

    List<String> result = new ParquetReader(fileSystemRule.getTargetDirPath(), fileSystemRule.getCloudFileSystem().getConf()).readAllInCsvSorted();
    assertEquals(getDataSet().getExpectedParquetOutput(), result);
  }

  @Test
  public void testImportAsParquetFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
    String[] args = getArgsWithAsParquetFileOption();
    runImport(args);

    args = getArgsWithAsParquetFileAndDeleteTargetDirOption();
    runImport(args);

    List<String> result = new ParquetReader(fileSystemRule.getTargetDirPath(), fileSystemRule.getCloudFileSystem().getConf()).readAllInCsvSorted();
    assertEquals(getDataSet().getExpectedParquetOutput(), result);
  }

  @Test
  public void testImportAsParquetFileWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
    String[] args = getArgsWithAsParquetFileOption();
    runImport(args);

    thrown.expect(IOException.class);
    runImport(args);
  }

  private String[] getArgsWithAsParquetFileOption() {
    return getArgsForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-parquetfile");
  }

  private String[] getArgsWithAsParquetFileAndDeleteTargetDirOption() {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), "as-parquetfile");
    builder.withOption("delete-target-dir");
    return builder.build();
  }
}
