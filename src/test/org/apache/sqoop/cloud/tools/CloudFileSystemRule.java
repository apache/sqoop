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

package org.apache.sqoop.cloud.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.UUID;

/**
 * The task of this rule is to initialize the cloud FileSystem object and the test directories
 * before the tests and clean them up after.
 * This FileSystem object is used to assert the output of a Sqoop job.
 * If the credentials are not provided for the cloud system the tests will be skipped.
 */
public class CloudFileSystemRule implements TestRule {

  private static final Log LOG = LogFactory.getLog(CloudFileSystemRule.class.getName());

  private static final String TARGET_DIR_NAME_PREFIX = "testdir";

  private static final String HIVE_EXTERNAL_DIR_NAME_PREFIX = "externaldir";

  private static final String TEMP_DIR = "tmp";

  private static final String TEMPORARY_ROOTDIR_SUFFIX = "_temprootdir";

  private final CloudCredentialsRule credentialsRule;

  private FileSystem fileSystem;

  private String targetDirName;

  private String hiveExternalTableDirName;

  public CloudFileSystemRule(CloudCredentialsRule credentialsRule) {
    this.credentialsRule = credentialsRule;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        setUp();
        try {
          base.evaluate();
        } finally {
          tearDown();
        }
      }
    };
  }

  public FileSystem getCloudFileSystem() {
    return fileSystem;
  }

  public Path getTargetDirPath() {
    return new Path(getCloudTempDirPath(), targetDirName);
  }

  public Path getTemporaryRootDirPath() {
    return new Path(getTargetDirPath().toString() + TEMPORARY_ROOTDIR_SUFFIX);
  }

  public Path getExternalTableDirPath() {
    return new Path(getCloudTempDirPath(), hiveExternalTableDirName);
  }

  private void setUp() throws IOException {
    initializeCloudFileSystem();
    initializeTestDirectoryNames();
  }

  private void initializeCloudFileSystem() throws IOException {
    Configuration hadoopConf = new Configuration();
    credentialsRule.addCloudCredentialProperties(hadoopConf);

    fileSystem = FileSystem.get(hadoopConf);
  }

  private void initializeTestDirectoryNames() {
    targetDirName = generateUniqueDirName(TARGET_DIR_NAME_PREFIX);
    hiveExternalTableDirName = generateUniqueDirName(HIVE_EXTERNAL_DIR_NAME_PREFIX);
  }

  private String generateUniqueDirName(String dirPrefix) {
    String uuid = UUID.randomUUID().toString();
    return dirPrefix + "-" + uuid;
  }

  private void cleanUpDirectory(Path directoryPath) {
    try {
      if (fileSystem.exists(directoryPath)) {
        fileSystem.delete(directoryPath, true);
      }
    } catch (Exception e) {
      LOG.error("Issue with cleaning up directory", e);
    }
  }

  private Path getCloudTempDirPath() {
    return new Path(credentialsRule.getBaseCloudDirectoryUrl(), TEMP_DIR);
  }

  private void tearDown() {
    cleanUpDirectory(getTemporaryRootDirPath());
    cleanUpDirectory(getTargetDirPath());
    cleanUpDirectory(getExternalTableDirPath());
  }
}
