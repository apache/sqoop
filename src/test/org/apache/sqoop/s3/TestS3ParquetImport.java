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

import static org.junit.Assert.assertEquals;

@Category(S3Test.class)
public class TestS3ParquetImport extends ImportJobTestCase {

    public static final Log LOG = LogFactory.getLog(
            TestS3ParquetImport.class.getName());

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
        S3TestUtils.createTestTableFromInputData(this);
        s3Client = S3TestUtils.setupS3ImportTestCase(s3CredentialGenerator);
    }

    @After
    public void cleanUpTargetDir() {
        S3TestUtils.tearDownS3ImportTestCase(s3Client);
        super.tearDown();
    }

    @Test
    public void testS3ImportAsParquetFileWithoutDeleteTargetDirOptionWhenTargetDirDoesNotExist() throws Exception {
        String[] args = getArgsWithAsParquetFileOption();
        runImport(args);

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();
        assertEquals(S3TestUtils.getExpectedParquetOutput(), result);
    }

    @Test
    public void testS3ImportAsParquetFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
        String[] args = getArgsWithAsParquetFileOption();
        runImport(args);

        args = getArgsWithAsParquetFileAndDeleteTargetDirOption();
        runImport(args);

        List<String> result = new ParquetReader(S3TestUtils.getTargetDirPath(), s3Client.getConf()).readAllInCsvSorted();
        assertEquals(S3TestUtils.getExpectedParquetOutput(), result);
    }

    @Test
    public void testS3ImportAsParquetFileWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
        String[] args = getArgsWithAsParquetFileOption();
        runImport(args);

        thrown.expect(IOException.class);
        runImport(args);
    }

    private String[] getArgsWithAsParquetFileOption() {
        return S3TestUtils.getArgsForS3UnitTestsWithFileFormatOption(this, s3CredentialGenerator, "as-parquetfile");
    }

    private String[] getArgsWithAsParquetFileAndDeleteTargetDirOption() {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForS3UnitTestsWithFileFormatOption(this,
                s3CredentialGenerator,"as-parquetfile");
        builder.withOption("delete-target-dir");
        return builder.build();
    }
}
