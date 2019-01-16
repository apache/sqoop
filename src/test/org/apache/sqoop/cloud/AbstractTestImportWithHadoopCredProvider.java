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

import static junit.framework.TestCase.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.TextFileTestUtils;
import org.apache.sqoop.util.password.CredentialProviderHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public abstract class AbstractTestImportWithHadoopCredProvider extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(AbstractTestImportWithHadoopCredProvider.class.getName());

  private static String providerPathDefault;
  private static String providerPathEnv;
  private static String providerPathPwdFile;

  protected static CloudCredentialsRule credentialsRule;

  @ClassRule
  public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();
  private static File providerFileDefault;
  private static File providerFileEnvPwd;
  private static File providerFilePwdFile;

  private static final String PASSWORD_FILE_NAME = "password-file.txt";
  private static final String HADOOP_CREDSTORE_PASSWORD_ENV_NAME = "HADOOP_CREDSTORE_PASSWORD";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final String credentialProviderPathProperty;

  @BeforeClass
  public static void fillCredentialProviderFiles() throws Exception {
    generateTempProviderFileNames();
    fillCredentialProviderDefault();
    fillCredentialProviderPwdFile();
    fillCredentialProviderEnv();
  }

  public AbstractTestImportWithHadoopCredProvider() {
    this(CredentialProviderHelper.HADOOP_CREDENTIAL_PROVIDER_PATH);
  }

  public AbstractTestImportWithHadoopCredProvider(String credentialProviderPathProperty) {
    super(credentialsRule);
    this.credentialProviderPathProperty = credentialProviderPathProperty;
  }

  @Before
  public void setup() {
    super.setUp();
    environmentVariables.clear(HADOOP_CREDSTORE_PASSWORD_ENV_NAME);
  }

  @AfterClass
  public static void deleteTemporaryCredFiles() {
    deleteFileOnExit(providerFileDefault);
    deleteFileOnExit(providerFileEnvPwd);
    deleteFileOnExit(providerFilePwdFile);
  }

  @Test
  public void testCredentialProviderDefaultSucceeds() throws Exception {
    runImport(getArgs(providerPathDefault, false, null));
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testCredentialProviderEnvSucceeds() throws Exception {
    setHadoopCredStorePwdEnvVar();
    runImport(getArgs(providerPathEnv, false, null));
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testCredentialProviderPwdFileSucceeds() throws Exception {
    runImport(getArgs(providerPathPwdFile, true, PASSWORD_FILE_NAME));
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testCredentialProviderWithNoProviderPathFails() throws Exception {
    thrown.expect(IOException.class);
    runImport(getArgs(null, false, null));
  }

  @Test
  public void testCredentialProviderWithNoEnvFails() throws Exception {
    thrown.expect(IOException.class);
    runImport(getArgs(providerPathEnv, false, null));
  }

  @Test
  public void testCredentialProviderWithWrongPwdFileFails() throws Exception {
    thrown.expect(IOException.class);
    runImport(getArgs(providerPathPwdFile, true, "wrong-password-file.txt"));
  }

  @Test
  public void testCredentialProviderWithNoPwdFileFails() throws Exception {
    thrown.expect(IOException.class);
    runImport(getArgs(providerPathPwdFile, true, null));
  }

  private String[] getArgs(String providerPath, boolean withPwdFile, String pwdFile) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForHadoopCredProviderUnitTests(fileSystemRule.getTargetDirPath().toString());

    builder.withProperty(credentialProviderPathProperty, providerPath);
    if (withPwdFile) {
      builder.withProperty(CredentialProviderHelper.CREDENTIAL_PROVIDER_PASSWORD_FILE, pwdFile);
    }
    return builder.build();
  }

  private static void fillCredentialProviderDefault() throws Exception {
    credentialsRule.fillCredentialProvider(new Configuration(), providerPathDefault);
  }

  private static void fillCredentialProviderEnv() throws Exception {
    setHadoopCredStorePwdEnvVar();
    credentialsRule.fillCredentialProvider(new Configuration(), providerPathEnv);
  }

  private static void fillCredentialProviderPwdFile() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PASSWORD_FILE, PASSWORD_FILE_NAME);
    credentialsRule.fillCredentialProvider(conf, providerPathPwdFile);
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

  private static void setHadoopCredStorePwdEnvVar() {
    environmentVariables.set(HADOOP_CREDSTORE_PASSWORD_ENV_NAME, "credProviderPwd");
  }

  private static void deleteFileOnExit(File file) {
    if (file != null) {
      file.deleteOnExit();
    }
  }

}
