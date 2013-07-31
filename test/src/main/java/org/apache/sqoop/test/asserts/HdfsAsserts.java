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
package org.apache.sqoop.test.asserts;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.test.utils.HdfsUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Assert methods suitable for checking HDFS files and directories.
 *
 * TODO: This module will require clean up to work on MiniCluster/Real cluster.
 */
public class HdfsAsserts {

  private static final Logger LOG = Logger.getLogger(HdfsAsserts.class);

  /**
   * Verify that mapreduce output (across all files) is as expected.
   *
   * @param directory Mapreduce output directory
   * @param lines Expected lines
   * @throws IOException
   */
  public static void assertMapreduceOutput(String directory, String... lines) throws IOException {
    Set<String> setLines = new HashSet<String>(Arrays.asList(lines));
    List<String> notFound = new LinkedList<String>();

    String []files = HdfsUtils.getOutputMapreduceFiles(directory);

    for(String file : files) {
      String filePath = directory + "/" + file;
      BufferedReader br = new BufferedReader(new FileReader((filePath)));

      String line;
      while ((line = br.readLine()) != null) {
        if (!setLines.remove(line)) {
          notFound.add(line);
        }
      }
      br.close();
    }

    if(!setLines.isEmpty() || !notFound.isEmpty()) {
      LOG.error("Expected lines that weren't present in the files:");
      LOG.error("\t" + StringUtils.join(setLines, "\n\t"));
      LOG.error("Extra lines in files that weren't expected:");
      LOG.error("\t" + StringUtils.join(notFound, "\n\t"));
      fail("Output do not match expectations.");
    }
  }

  /**
   * Verify number of output mapreduce files.
   *
   * @param directory Mapreduce output directory
   * @param expectedFiles Expected number of files
   */
  public static void assertMapreduceOutputFiles(String directory, int expectedFiles) {
    String []files = HdfsUtils.getOutputMapreduceFiles(directory);
    assertEquals(expectedFiles, files.length);
  }

  private HdfsAsserts() {
    // Instantiation is prohibited
  }
}
