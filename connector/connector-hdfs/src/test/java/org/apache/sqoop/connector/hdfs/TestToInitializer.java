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
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class TestToInitializer extends TestHdfsBase {

  @Test
  public void testWorkDirectoryBeingSet() {
    final String TARGET_DIR = "/target/directory";

    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.uri = "file:///";
    jobConfig.toJobConfig.outputDirectory = TARGET_DIR;

    InitializerContext initializerContext = new InitializerContext(new MutableMapContext());

    Initializer initializer = new HdfsToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    assertNotNull(initializerContext.getString(HdfsConstants.WORK_DIRECTORY));
    assertTrue(initializerContext.getString(HdfsConstants.WORK_DIRECTORY).startsWith(TARGET_DIR + "/."));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testOutputDirectoryIsAFile() throws Exception {
    File file = File.createTempFile("MastersOfOrion", ".txt");
    file.createNewFile();

    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.uri = "file:///";
    jobConfig.toJobConfig.outputDirectory = file.getAbsolutePath();

    InitializerContext initializerContext = new InitializerContext(new MutableMapContext());

    Initializer initializer = new HdfsToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testOutputDirectoryIsNotEmpty() throws Exception {
    File dir = Files.createTempDir();
    File file = File.createTempFile("MastersOfOrion", ".txt", dir);

    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.uri = "file:///";
    jobConfig.toJobConfig.outputDirectory = dir.getAbsolutePath();

    InitializerContext initializerContext = new InitializerContext(new MutableMapContext());

    Initializer initializer = new HdfsToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);
  }

  @Test
  public void testOutputDirectoryIsNotEmptyWithIncremental() throws Exception {
    File dir = Files.createTempDir();
    File file = File.createTempFile("MastersOfOrion", ".txt", dir);

    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();

    linkConfig.linkConfig.uri = "file:///";
    jobConfig.toJobConfig.outputDirectory = dir.getAbsolutePath();
    jobConfig.toJobConfig.appendMode = true;

    InitializerContext initializerContext = new InitializerContext(new MutableMapContext());

    Initializer initializer = new HdfsToInitializer();
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    assertNotNull(initializerContext.getString(HdfsConstants.WORK_DIRECTORY));
    assertTrue(initializerContext.getString(HdfsConstants.WORK_DIRECTORY).startsWith(dir.getAbsolutePath()));
  }
}
