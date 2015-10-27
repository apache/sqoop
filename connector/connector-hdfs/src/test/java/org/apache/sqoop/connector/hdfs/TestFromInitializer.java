/**
 * Created by jarcec on 8/18/15.
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
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.IncrementalType;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestFromInitializer {

  Initializer<LinkConfiguration, FromJobConfiguration> initializer;
  InitializerContext initializerContext;
  LinkConfiguration linkConfig;
  FromJobConfiguration jobConfig;
  MutableContext context;

  public TestFromInitializer() {
    linkConfig = new LinkConfiguration();
    jobConfig = new FromJobConfiguration();
    context = new MutableMapContext();
    initializer = new HdfsFromInitializer();
    initializerContext = new InitializerContext(context, "test_user");
  }

  @Test
  public void testConfigOverrides() {
    linkConfig.linkConfig.uri = "file:///";
    linkConfig.linkConfig.configOverrides.put("key", "value");
    jobConfig.fromJobConfig.inputDirectory = "/tmp";

    initializer.initialize(initializerContext, linkConfig, jobConfig);

    assertEquals(initializerContext.getString("key"), "value");
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testFailIfInputDirectoryDoNotExists() {
    jobConfig.fromJobConfig.inputDirectory = "/tmp/this/directory/definitely/do/not/exists";
    initializer.initialize(initializerContext, linkConfig, jobConfig);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testFailIfInputDirectoryIsFile() throws Exception {
    File workDir = Files.createTempDir();
    File inputFile = File.createTempFile("part-01-", ".txt", workDir);
    inputFile.createNewFile();

    jobConfig.fromJobConfig.inputDirectory = inputFile.getAbsolutePath();
    initializer.initialize(initializerContext, linkConfig, jobConfig);
  }

  @Test
  public void testIncremental() throws Exception {
    File workDir = Files.createTempDir();
    File.createTempFile("part-01-", ".txt", workDir).createNewFile();
    File.createTempFile("part-02-", ".txt", workDir).createNewFile();

    jobConfig.fromJobConfig.inputDirectory = workDir.getAbsolutePath();
    jobConfig.incremental.incrementalType = IncrementalType.NEW_FILES;
    initializer.initialize(initializerContext, linkConfig, jobConfig);

    // Max import date must be defined if we are running incremental
    assertNotNull(context.getString(HdfsConstants.MAX_IMPORT_DATE));
  }
}
