/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.s3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.sqoop.testcategories.thirdpartytest.S3Test;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.DefaultS3CredentialGenerator;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.S3CredentialGenerator;
import org.apache.sqoop.testutil.S3TestUtils;
import org.apache.sqoop.util.ParquetReader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

import static org.apache.sqoop.util.AppendUtils.MAPREDUCE_OUTPUT_BASENAME_PROPERTY;
import static org.junit.Assert.assertEquals;

@Category(S3Test.class)
public class TestS3IncrementalMergeParquetImport extends ImportJobTestCase {

    public static final Log LOG = LogFactory.getLog(
            TestS3IncrementalMergeParquetImport.class.getName());

    private static S3CredentialGenerator s3CredentialGenerator;

    private FileSystem s3Client;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupS3Credentials() throws IOException {
        String generatorCommand = S3TestUtils.getGeneratorCommand();
        if (generatorCommand != null) {
            s3CredentialGenerator = new DefaultS3CredentialGenerator(generatorCommand);
        }
    }

    @Before
    public void setup() throws IOException {
        S3TestUtils.runTestCaseOnlyIfS3CredentialsAreSet(s3CredentialGenerator);
        super.setUp();
        S3TestUtils.createTestTableFromInitialInputDataForMerge(this);
        s3Client = S3TestUtils.setupS3ImportTestCase(s3CredentialGenerator);
    }

    @After
    public void cleanUpOutputDirectories() {
        S3TestUtils.tearDownS3IncrementalImportTestCase(s3Client);
        super.tearDown();
    }

    @Test
    public void testS3IncrementalMergeAsParquetFileWhenNoNewRowIsImported() throws Exception {
        String[] args = getArgsWithAsParquetFileOption(false);
        runImport(args);

        clearTable(getTableName());

        args = getIncrementalMergeArgsWithAsParquetFileOption(false);
        runImport(args);

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();
        assertEquals(S3TestUtils.getExpectedParquetOutputWithTimestampColumn(this), result);
    }

    @Test
    public void testS3IncrementalMergeAsParquetFile() throws Exception {
        String[] args = getArgsWithAsParquetFileOption(false);
        runImport(args);

        clearTable(getTableName());

        S3TestUtils.insertInputDataIntoTableForMerge(this, S3TestUtils.getNewInputDataForMerge());

        args = getIncrementalMergeArgsWithAsParquetFileOption(false);
        runImport(args);

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();
        assertEquals(S3TestUtils.getExpectedParquetOutputWithTimestampColumnAfterMerge(this), result);
    }

    @Test
    public void testS3IncrementalMergeAsParquetFileWithMapreduceOutputBasenameProperty() throws Exception {
        String[] args = getArgsWithAsParquetFileOption(true);
        runImport(args);

        clearTable(getTableName());

        S3TestUtils.insertInputDataIntoTableForMerge(this, S3TestUtils.getNewInputDataForMerge());

        args = getIncrementalMergeArgsWithAsParquetFileOption(true);
        runImport(args);

        S3TestUtils.failIfOutputFilePathContainingPatternDoesNotExists(s3Client, S3TestUtils.MAPREDUCE_OUTPUT_BASENAME);

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();
        assertEquals(S3TestUtils.getExpectedParquetOutputWithTimestampColumnAfterMerge(this), result);
    }

    private String[] getArgsWithAsParquetFileOption(boolean withMapreduceOutputBasenameProperty) {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForS3UnitTestsWithFileFormatOption(this,
                s3CredentialGenerator,"as-parquetfile");
        if (withMapreduceOutputBasenameProperty) {
            builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, S3TestUtils.MAPREDUCE_OUTPUT_BASENAME);
        }
        return builder.build();
    }

    private String[] getIncrementalMergeArgsWithAsParquetFileOption(boolean withMapreduceOutputBasenameProperty) {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForS3UnitTestsWithFileFormatOption(this,
                s3CredentialGenerator,"as-parquetfile");
        builder = S3TestUtils.addIncrementalMergeImportArgs(builder);
        if (withMapreduceOutputBasenameProperty) {
            builder.withProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY, S3TestUtils.MAPREDUCE_OUTPUT_BASENAME);
        }
        return builder.build();
    }
}
