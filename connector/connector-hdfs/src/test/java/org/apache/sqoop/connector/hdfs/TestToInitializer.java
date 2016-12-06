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
package org.apache.sqoop.connector.hdfs;

import com.google.common.io.Files;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class TestToInitializer extends TestHdfsBase {

  private LinkConfiguration linkConfig;
  private ToJobConfiguration jobConfig;
  private InitializerContext initializerContext;
  private Initializer initializer;

  @BeforeMethod
  public void setup() {
    linkConfig = new LinkConfiguration();
    jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.uri = "file:///";

    initializerContext = new InitializerContext(new MutableMapContext(), "test_user");
    initializer= new HdfsToInitializer();
  }

  @Test
  public void testWorkDirectoryBeingSet() {
    final String TARGET_DIR = "/target/directory";

    jobConfig.toJobConfig.outputDirectory = TARGET_DIR;

    initializer.initialize(initializerContext, linkConfig, jobConfig);

    assertNotNull(initializerContext.getString(HdfsConstants.WORK_DIRECTORY));
    assertTrue(initializerContext.getString(HdfsConstants.WORK_DIRECTORY).startsWith(TARGET_DIR + "/."));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testOutputDirectoryIsAFile() throws Exception {
    File file = File.createTempFile("MastersOfOrion", ".txt");
    file.createNewFile();

    jobConfig.toJobConfig.outputDirectory = file.getAbsolutePath();

    initializer.initialize(initializerContext, linkConfig, jobConfig);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testOutputDirectoryIsNotEmptyWithoutDeleteOption() throws Exception {
    File dir = Files.createTempDir();
    File file = File.createTempFile("MastersOfOrion", ".txt", dir);

    jobConfig.toJobConfig.outputDirectory = dir.getAbsolutePath();

    initializer.initialize(initializerContext, linkConfig, jobConfig);
  }

  @Test
  public void testOutputDirectoryIsNotEmptyWithDeleteOption() throws Exception {
    File dir = Files.createTempDir();
    File.createTempFile("MastersOfOrion", ".txt", dir);

    jobConfig.toJobConfig.outputDirectory = dir.getAbsolutePath();
    jobConfig.toJobConfig.deleteOutputDirectory = true;

    initializer.initialize(initializerContext, linkConfig, jobConfig);

    assertNotNull(initializerContext.getString(HdfsConstants.WORK_DIRECTORY));
    assertTrue(initializerContext.getString(HdfsConstants.WORK_DIRECTORY).startsWith(dir.getAbsolutePath() + "/."));
  }

  @Test
  public void testOutputDirectoryIsNotEmptyWithIncremental() throws Exception {
    File dir = Files.createTempDir();
    File.createTempFile("MastersOfOrion", ".txt", dir);

    jobConfig.toJobConfig.outputDirectory = dir.getAbsolutePath();
    jobConfig.toJobConfig.appendMode = true;

    initializer.initialize(initializerContext, linkConfig, jobConfig);

    assertNotNull(initializerContext.getString(HdfsConstants.WORK_DIRECTORY));
    assertTrue(initializerContext.getString(HdfsConstants.WORK_DIRECTORY).startsWith(dir.getAbsolutePath()));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testOutputDirectoryIsNotEmptyWithoutIncrementalWithoutDeleteOption() throws Exception {
    File dir = Files.createTempDir();
    File.createTempFile("MastersOfOrion", ".txt", dir);

    jobConfig.toJobConfig.outputDirectory = dir.getAbsolutePath();
    jobConfig.toJobConfig.appendMode = false;

    initializer.initialize(initializerContext, linkConfig, jobConfig);
  }

  @Test
  public void testOutputDirectoryIsNotEmptyWithoutIncrementalWithDeleteOption() throws Exception {
    File dir = Files.createTempDir();
    File.createTempFile("MastersOfOrion", ".txt", dir);

    jobConfig.toJobConfig.outputDirectory = dir.getAbsolutePath();
    jobConfig.toJobConfig.appendMode = false;
    jobConfig.toJobConfig.deleteOutputDirectory = true;

    initializer.initialize(initializerContext, linkConfig, jobConfig);

    assertNotNull(initializerContext.getString(HdfsConstants.WORK_DIRECTORY));
    assertTrue(initializerContext.getString(HdfsConstants.WORK_DIRECTORY).startsWith(dir.getAbsolutePath()));
  }
}
