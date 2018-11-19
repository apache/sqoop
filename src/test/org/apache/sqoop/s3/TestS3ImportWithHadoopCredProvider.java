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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.util.ToolRunner;
import org.apache.sqoop.testcategories.thirdpartytest.S3Test;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.DefaultS3CredentialGenerator;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.S3CredentialGenerator;
import org.apache.sqoop.testutil.S3TestUtils;
import org.apache.sqoop.testutil.TextFileTestUtils;
import org.apache.sqoop.util.password.CredentialProviderHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static junit.framework.TestCase.fail;

@Category(S3Test.class)
public class TestS3ImportWithHadoopCredProvider extends ImportJobTestCase {
    public static final Log LOG = LogFactory.getLog(
            TestS3ImportWithHadoopCredProvider.class.getName());

    private static S3CredentialGenerator s3CredentialGenerator;

    private static String providerPathDefault;
    private static String providerPathEnv;
    private static String providerPathPwdFile;

    @ClassRule
    public static final EnvironmentVariables environmentVariables
            = new EnvironmentVariables();
    private static File providerFileDefault;
    private static File providerFileEnvPwd;
    private static File providerFilePwdFile;

    private FileSystem s3Client;

    private static final String PASSWORD_FILE_NAME = "password-file.txt";
    private static final String HADOOP_CREDSTORE_PASSWORD_ENV_NAME = "HADOOP_CREDSTORE_PASSWORD";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupS3Credentials() throws Exception {
        String generatorCommand = S3TestUtils.getGeneratorCommand();
        if (generatorCommand != null) {
            s3CredentialGenerator = new DefaultS3CredentialGenerator(generatorCommand);
            generateTempProviderFileNames();
            fillCredentialProviderDefault();
            fillCredentialProviderPwdFile();
            fillCredentialProviderEnv();
        }
    }

    @Before
    public void setup() throws IOException {
        S3TestUtils.runTestCaseOnlyIfS3CredentialsAreSet(s3CredentialGenerator);
        super.setUp();
        S3TestUtils.createTestTableFromInputData(this);
        s3Client = S3TestUtils.setupS3ImportTestCase(s3CredentialGenerator);
        environmentVariables.clear(HADOOP_CREDSTORE_PASSWORD_ENV_NAME);
    }

    @After
    public void cleanUpTargetDir() {
        S3TestUtils.tearDownS3ImportTestCase(s3Client);
        super.tearDown();
    }

    @AfterClass
    public static void deleteTemporaryCredFiles() {
        deleteFileOnExit(providerFileDefault);
        deleteFileOnExit(providerFileEnvPwd);
        deleteFileOnExit(providerFilePwdFile);
    }

    @Test
    public void testCredentialProviderDefaultSucceeds() throws Exception {
        runImport(getArgs(providerPathDefault,false, null));
        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutput(), s3Client, S3TestUtils.getTargetDirPath());
    }

    @Test
    public void testCredentialProviderEnvSucceeds() throws Exception {
        setHadoopCredStorePwdEnvVar();
        runImport(getArgs(providerPathEnv,false, null));
        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutput(), s3Client, S3TestUtils.getTargetDirPath());
    }

    @Test
    public void testCredentialProviderPwdFileSucceeds() throws Exception {
        runImport(getArgs(providerPathPwdFile,true, PASSWORD_FILE_NAME));
        TextFileTestUtils.verify(S3TestUtils.getExpectedTextOutput(), s3Client, S3TestUtils.getTargetDirPath());
    }

    @Test
    public void testCredentialProviderWithNoProviderPathFails() throws Exception {
        thrown.expect(IOException.class);
        runImport(getArgs(null,false, null));
    }

    @Test
    public void testCredentialProviderWithNoEnvFails() throws Exception {
        thrown.expect(IOException.class);
        runImport(getArgs(providerPathEnv,false, null));
    }

    @Test
    public void testCredentialProviderWithWrongPwdFileFails() throws Exception {
        thrown.expect(IOException.class);
        runImport(getArgs(providerPathPwdFile,true, "wrong-password-file.txt"));
    }

    @Test
    public void testCredentialProviderWithNoPwdFileFails() throws Exception {
        thrown.expect(IOException.class);
        runImport(getArgs(providerPathPwdFile,true, null));
    }

    private String[] getArgs(String providerPath, boolean withPwdFile, String pwdFile) {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForHadoopCredProviderS3UnitTests(this);

        builder.withProperty(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH, providerPath);
        if (withPwdFile) {
            builder.withProperty(CredentialProviderHelper.CREDENTIAL_PROVIDER_PASSWORD_FILE, pwdFile);
        }
        return builder.build();
    }

    private static void fillCredentialProviderDefault() throws Exception {
        fillCredentialProvider(new Configuration(), providerPathDefault);
    }

    private static void fillCredentialProviderEnv() throws Exception {
        setHadoopCredStorePwdEnvVar();
        fillCredentialProvider(new Configuration(), providerPathEnv);
    }

    private static void fillCredentialProviderPwdFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PASSWORD_FILE, PASSWORD_FILE_NAME);
        fillCredentialProvider(conf, providerPathPwdFile);
    }

    private static void generateTempProviderFileNames() throws IOException {
        providerFileDefault = Files.createTempFile("test-default-pwd-", ".jceks").toFile();
        boolean deleted = providerFileDefault.delete();
        providerFileEnvPwd = Files.createTempFile("test-env-pwd-", ".jceks").toFile();
        deleted &= providerFileEnvPwd.delete();
        providerFilePwdFile = Files.createTempFile("test-file-pwd-", ".jceks").toFile();
        deleted &= providerFilePwdFile.delete();
        if (!deleted) {
            fail("Could not delete temporary provider files");
        }
        providerPathDefault = "jceks://file/" + providerFileDefault.getAbsolutePath();
        providerPathEnv = "jceks://file/" + providerFileEnvPwd.getAbsolutePath();
        providerPathPwdFile = "jceks://file/" + providerFilePwdFile.getAbsolutePath();
    }

    private static void runCredentialProviderCreateCommand(String command, Configuration conf) throws Exception {
        ToolRunner.run(conf, new CredentialShell(), command.split(" "));
    }

    private static String getCreateCommand(String credentialKey, String credentialValue, String providerPath) {
        return "create " + credentialKey + " -value " + credentialValue + " -provider " + providerPath;
    }

    private static void fillCredentialProvider(Configuration conf, String providerPath) throws Exception {
        runCredentialProviderCreateCommand(getCreateCommand(Constants.ACCESS_KEY, s3CredentialGenerator.getS3AccessKey(), providerPath), conf);
        runCredentialProviderCreateCommand(getCreateCommand(Constants.SECRET_KEY, s3CredentialGenerator.getS3SecretKey(), providerPath), conf);

        if (s3CredentialGenerator.getS3SessionToken() != null) {
            runCredentialProviderCreateCommand(getCreateCommand(Constants.SESSION_TOKEN, s3CredentialGenerator.getS3SessionToken(), providerPath), conf);
        }
    }

    private static void setHadoopCredStorePwdEnvVar() {
        environmentVariables.set(HADOOP_CREDSTORE_PASSWORD_ENV_NAME, "credProviderPwd");
    }

    private static void deleteFileOnExit(File file) {
        if (file != null) {
            file.deleteOnExit();
        }
    }
}
