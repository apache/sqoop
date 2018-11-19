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
import org.apache.sqoop.testutil.SequenceFileTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;

@Category(S3Test.class)
public class TestS3SequenceFileImport extends ImportJobTestCase {

    public static final Log LOG = LogFactory.getLog(
            TestS3SequenceFileImport.class.getName());

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
    public void testS3ImportAsSequenceFileWithoutDeleteTargetDirOptionWhenTargetDirDoesNotExist() throws Exception {
        String[] args = getArgsWithAsSequenceFileOption();
        runImport(args);
        SequenceFileTestUtils.verify(this, S3TestUtils.getExpectedSequenceFileOutput(), s3Client, S3TestUtils.getTargetDirPath());
    }

    @Test
    public void testS3ImportAsSequenceFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
        String[] args = getArgsWithAsSequenceFileOption();
        runImport(args);

        args = getArgsWithAsSequenceFileAndDeleteTargetDirOption();
        runImport(args);
        SequenceFileTestUtils.verify(this, S3TestUtils.getExpectedSequenceFileOutput(), s3Client, S3TestUtils.getTargetDirPath());
    }

    @Test
    public void testS3ImportAsSequenceWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws Exception {
        String[] args = getArgsWithAsSequenceFileOption();
        runImport(args);

        thrown.expect(IOException.class);
        runImport(args);
    }

    private String[] getArgsWithAsSequenceFileOption() {
        return S3TestUtils.getArgsForS3UnitTestsWithFileFormatOption(this, s3CredentialGenerator, "as-sequencefile");
    }

    private String[] getArgsWithAsSequenceFileAndDeleteTargetDirOption() {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForS3UnitTestsWithFileFormatOption(this,
                s3CredentialGenerator,"as-sequencefile");
        builder.withOption("delete-target-dir");
        return builder.build();
    }
}
