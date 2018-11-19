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
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.testcategories.thirdpartytest.S3Test;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.DefaultS3CredentialGenerator;
import org.apache.sqoop.testutil.HiveServer2TestUtil;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.S3CredentialGenerator;
import org.apache.sqoop.testutil.S3TestUtils;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.sqoop.tool.BaseSqoopTool.FMT_PARQUETFILE_ARG;
import static org.apache.sqoop.tool.BaseSqoopTool.FMT_TEXTFILE_ARG;
import static org.junit.Assert.assertEquals;

@Category(S3Test.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestS3ExternalHiveTableImport extends ImportJobTestCase {

    @Parameterized.Parameters(name = "fileFormatArg = {0}, expectedResult = {1}")
    public static Iterable<? extends Object> parameters() {
        return Arrays.asList(new Object[] {FMT_TEXTFILE_ARG, S3TestUtils.getExpectedTextOutputAsList()},
                new Object[] {FMT_PARQUETFILE_ARG, S3TestUtils.getExpectedParquetOutput()});
    }

    public static final Log LOG = LogFactory.getLog(
            TestS3ExternalHiveTableImport.class.getName());

    private String fileFormatArg;

    private List<String> expectedResult;

    public TestS3ExternalHiveTableImport(String fileFormatArg, List<String> expectedResult) {
        this.fileFormatArg = fileFormatArg;
        this.expectedResult = expectedResult;
    }

    private static S3CredentialGenerator s3CredentialGenerator;

    private FileSystem s3Client;

    private HiveMiniCluster hiveMiniCluster;

    private static HiveServer2TestUtil hiveServer2TestUtil;

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
        hiveMiniCluster = S3TestUtils.setupS3ExternalHiveTableImportTestCase(s3CredentialGenerator);
        hiveServer2TestUtil = new HiveServer2TestUtil(hiveMiniCluster.getUrl());
    }

    @After
    public void cleanUpTargetDir() {
        S3TestUtils.tearDownS3ExternalHiveTableImportTestCase(s3Client);
        super.tearDown();
        if (hiveMiniCluster != null) {
            hiveMiniCluster.stop();
        }
    }

    @Test
    public void testS3ImportIntoExternalHiveTable() throws IOException {
        String[] args = getExternalHiveTableImportArgs(false);
        runImport(args);

        List<String> rows = hiveServer2TestUtil.loadCsvRowsFromTable(getTableName());
        assertEquals(rows, expectedResult);
    }

    @Test
    public void testS3CreateAndImportIntoExternalHiveTable() throws IOException {
        String[] args = getExternalHiveTableImportArgs(true);
        runImport(args);

        List<String> rows = hiveServer2TestUtil.loadCsvRowsFromTable(S3TestUtils.HIVE_EXTERNAL_TABLE_NAME);
        assertEquals(rows, expectedResult);
    }

    private String[] getExternalHiveTableImportArgs(boolean createHiveTable) {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForS3UnitTestsWithFileFormatOption(this,
                s3CredentialGenerator, fileFormatArg);
        builder = S3TestUtils.addExternalHiveTableImportArgs(builder, hiveMiniCluster.getUrl());
        if(createHiveTable) {
            builder = S3TestUtils.addCreateHiveTableArgs(builder);
        }
        return builder.build();
    }

}
