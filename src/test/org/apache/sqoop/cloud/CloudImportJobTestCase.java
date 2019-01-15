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

import static org.apache.sqoop.tool.BaseSqoopTool.FMT_PARQUETFILE_ARG;
import static org.apache.sqoop.util.AppendUtils.MAPREDUCE_OUTPUT_BASENAME_PROPERTY;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.cloud.tools.CloudFileSystemRule;
import org.apache.sqoop.cloud.tools.CloudTestDataSet;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.NoAuthenticationConfiguration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.util.FileSystemUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.List;

public abstract class CloudImportJobTestCase extends ImportJobTestCase {

  public static final String MAPREDUCE_OUTPUT_BASENAME = "custom";
  public static final String CUSTOM_MAP_OUTPUT_FILE_00001 = MAPREDUCE_OUTPUT_BASENAME + "-m-00001";
  public static final String CUSTOM_REDUCE_OUTPUT_FILE_00000 = MAPREDUCE_OUTPUT_BASENAME + "-r-00000";

  private static final String HIVE_EXTERNAL_TABLE_NAME = "test_external_table";

  private final CloudTestDataSet dataSet;

  private final CloudCredentialsRule credentialsRule;

  @Rule
  public CloudFileSystemRule fileSystemRule;

  protected CloudImportJobTestCase(CloudCredentialsRule credentialsRule) {
    this.credentialsRule = credentialsRule;
    this.fileSystemRule = new CloudFileSystemRule(credentialsRule);
    this.dataSet = new CloudTestDataSet();
  }

  @Override
  @Before
  public void setUp() {
    super.setUp();
    createTestTableFromInputData();
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
    System.clearProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY);
  }

  protected ArgumentArrayBuilder getArgumentArrayBuilderForUnitTests(String targetDir) {
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    credentialsRule.addCloudCredentialProperties(builder);
    return builder.withCommonHadoopFlags()
        .withOption("connect", getConnectString())
        .withOption("num-mappers", "1")
        .withOption("table", getTableName())
        .withOption("target-dir", targetDir);
  }

  protected ArgumentArrayBuilder getArgumentArrayBuilderForHadoopCredProviderUnitTests(String targetDir) {

    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    credentialsRule.addCloudCredentialProviderProperties(builder);
    return builder.withCommonHadoopFlags()
        .withOption("connect", getConnectString())
        .withOption("num-mappers", "1")
        .withOption("table", getTableName())
        .withOption("target-dir", targetDir);
  }

  protected ArgumentArrayBuilder getArgumentArrayBuilderForUnitTestsWithFileFormatOption(String targetDir,
                                                                                         String fileFormat) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(targetDir);
    builder.withOption(fileFormat);
    useParquetHadoopAPIImplementationIfAsParquet(builder, fileFormat);
    return builder;
  }

  protected String[] getArgsForUnitTestsWithFileFormatOption(String targetDir,
                                                             String fileFormat) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(targetDir);
    builder.withOption(fileFormat);
    useParquetHadoopAPIImplementationIfAsParquet(builder, fileFormat);
    return builder.build();
  }

  private void useParquetHadoopAPIImplementationIfAsParquet(ArgumentArrayBuilder builder, String fileFormat) {
    // Parquet import to Cloud storages is supported only with the Parquet Hadoop API implementation.
    if (fileFormat.equals(FMT_PARQUETFILE_ARG)) {
      builder.withOption("parquet-configurator-implementation", "hadoop");
    }
  }

  protected ArgumentArrayBuilder addExternalHiveTableImportArgs(ArgumentArrayBuilder builder,
                                                                String hs2Url,
                                                                String externalTableDir) {
    return builder
        .withOption("hive-import")
        .withOption("hs2-url", hs2Url)
        .withOption("external-table-dir", externalTableDir);
  }

  protected ArgumentArrayBuilder addCreateHiveTableArgs(ArgumentArrayBuilder builder, String hiveExternalTableDirName) {
    return builder
        .withOption("create-hive-table")
        .withOption("hive-table", hiveExternalTableDirName);
  }

  protected ArgumentArrayBuilder addIncrementalAppendImportArgs(ArgumentArrayBuilder builder, String temporaryRootDir) {
    return builder
        .withOption("incremental", "append")
        .withOption("check-column", "ID")
        .withOption("last-value", "4")
        .withOption("temporary-rootdir", temporaryRootDir);
  }

  protected ArgumentArrayBuilder addIncrementalMergeImportArgs(ArgumentArrayBuilder builder, String temporaryRootDir) {
    return builder
        .withOption("incremental", "lastmodified")
        .withOption("check-column", "RECORD_DATE")
        .withOption("merge-key", "DEBUT")
        .withOption("last-value", dataSet.getInitialTimestampForMerge())
        .withOption("temporary-rootdir", temporaryRootDir);
  }

  protected void createTestTableFromInputData() {
    List<String[]> inputData = dataSet.getInputData();
    createTableWithColTypesAndNames(dataSet.getColumnNames(), dataSet.getColumnTypes(), new String[0]);
    for (String[] dataRow : inputData) {
      insertInputDataIntoTable(dataRow);
    }
  }

  protected void insertInputDataIntoTable(String[] inputData) {
    insertIntoTable(dataSet.getColumnNames(), dataSet.getColumnTypes(), inputData);
  }

  protected void createTestTableFromInitialInputDataForMerge() {
    createTableWithRecordsWithColTypesAndNames(dataSet.getColumnNamesForMerge(), dataSet.getColumnTypesForMerge(), dataSet.getInitialInputDataForMerge());
  }

  protected void insertInputDataIntoTableForMerge(List<List<Object>> inputData) {
    insertRecordsIntoTableWithColTypesAndNames(dataSet.getColumnNamesForMerge(), dataSet.getColumnTypesForMerge(), inputData);
  }

  protected void failIfOutputFilePathContainingPatternExists(FileSystem cloudFileSystem, Path targetDirPath, String pattern) throws IOException {
    List<Path> outputFilesWithPathContainingPattern = FileSystemUtil.findFilesWithPathContainingPattern(targetDirPath,
        cloudFileSystem.getConf(), pattern);
    if (outputFilesWithPathContainingPattern.size() != 0) {
      fail("No output file was expected with pattern" + pattern);
    }
  }

  protected void failIfOutputFilePathContainingPatternDoesNotExists(FileSystem cloudFileSystem, Path targetDirPath, String pattern) throws IOException {
    List<Path> outputFilesWithPathContainingPattern = FileSystemUtil.findFilesWithPathContainingPattern(targetDirPath,
        cloudFileSystem.getConf(), pattern);
    if (outputFilesWithPathContainingPattern.size() == 0) {
      fail("No output file was found with pattern" + pattern);
    }
  }

  protected CloudTestDataSet getDataSet() {
    return dataSet;
  }

  protected HiveMiniCluster createCloudBasedHiveMiniCluster() {
    HiveMiniCluster hiveMiniCluster = new HiveMiniCluster(new NoAuthenticationConfiguration());
    hiveMiniCluster.start();
    credentialsRule.addCloudCredentialProperties(hiveMiniCluster.getConfig());

    return hiveMiniCluster;
  }

  protected String getHiveExternalTableName() {
    return HIVE_EXTERNAL_TABLE_NAME;
  }

  protected CloudCredentialsRule getCredentialsRule() {
    return credentialsRule;
  }
}
