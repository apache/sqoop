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
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 */
public class TestToDestroyer {

  @Test
  public void testDestroyOnSuccess() throws Exception {
    File workDir = Files.createTempDir();
    File targetDir = Files.createTempDir();

    File.createTempFile("part-01-", ".txt", workDir).createNewFile();
    File.createTempFile("part-02-", ".txt", workDir).createNewFile();
    File.createTempFile("part-03-", ".txt", workDir).createNewFile();

    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();
    jobConfig.toJobConfig.outputDirectory = targetDir.getAbsolutePath();

    MutableContext context = new MutableMapContext();
    context.setString(HdfsConstants.WORK_DIRECTORY, workDir.getAbsolutePath());

    Destroyer destroyer = new HdfsToDestroyer();
    destroyer.destroy(new DestroyerContext(context, true, null), linkConfig, jobConfig);

    File[] files = targetDir.listFiles();

    // We should see three files in the target directory
    assertNotNull(files);
    assertEquals(files.length, 3);

    // With expected file names
    boolean f1 = false, f2 = false, f3 = false;
    for(File f : files) {
      if(f.getName().startsWith("part-01-")) {
        f1 = true;
      }
      if(f.getName().startsWith("part-02-")) {
        f2 = true;
      }
      if(f.getName().startsWith("part-03-")) {
        f3 = true;
      }
    }
    assertTrue(f1);
    assertTrue(f2);
    assertTrue(f3);

    // And target directory should not exists
    assertFalse(workDir.exists());
  }
  @Test
  public void testDestroyOnFailure() throws Exception {
    File workDir = Files.createTempDir();
    File targetDir = Files.createTempDir();

    File.createTempFile("part-01-", ".txt", workDir).createNewFile();
    File.createTempFile("part-02-", ".txt", workDir).createNewFile();
    File.createTempFile("part-03-", ".txt", workDir).createNewFile();

    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration jobConfig = new ToJobConfiguration();
    jobConfig.toJobConfig.outputDirectory = targetDir.getAbsolutePath();

    MutableContext context = new MutableMapContext();
    context.setString(HdfsConstants.WORK_DIRECTORY, workDir.getAbsolutePath());

    Destroyer destroyer = new HdfsToDestroyer();
    destroyer.destroy(new DestroyerContext(context, false, null), linkConfig, jobConfig);

    File[] files = targetDir.listFiles();

    // We should see no files in the target directory
    assertNotNull(files);
    assertEquals(files.length, 0);

    // And target directory should not exists
    assertFalse(workDir.exists());
  }

}
