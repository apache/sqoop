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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.sqoop.test.utils.HdfsUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.fail;

/**
 * Assert methods suitable for checking HDFS files and directories.
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
  public static void assertMapreduceOutput(FileSystem fs, String directory, String... lines) throws IOException {
    Multiset<String> setLines = HashMultiset.create(Arrays.asList(lines));
    List<String> notFound = new LinkedList<String>();

    Path[] files = HdfsUtils.getOutputMapreduceFiles(fs, directory);
    for(Path file : files) {
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));

      String line;
      while ((line = br.readLine()) != null) {
        if (!setLines.remove(line)) {
          notFound.add(line);
        }
      }
      br.close();
    }

    if(!setLines.isEmpty() || !notFound.isEmpty()) {
      LOG.error("Output do not match expectations.");
      LOG.error("Expected lines that weren't present in the files:");
      LOG.error("\t'" + StringUtils.join(setLines, "'\n\t'") + "'");
      LOG.error("Extra lines in files that weren't expected:");
      LOG.error("\t'" + StringUtils.join(notFound, "'\n\t'") + "'");
      fail("Output do not match expectations.");
    }
  }

  /**
   * Verify number of output mapreduce files.
   *
   * @param directory Mapreduce output directory
   * @param expectedFiles Expected number of files
   */
  public static void assertMapreduceOutputFiles(FileSystem fs, String directory, int expectedFiles) throws IOException {
    Path[] files = HdfsUtils.getOutputMapreduceFiles(fs, directory);
    assertEquals(expectedFiles, files.length);
  }

  private HdfsAsserts() {
    // Instantiation is prohibited
  }
}
