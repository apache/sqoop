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

package org.apache.sqoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import org.apache.sqoop.testutil.CommonArgs;
import org.apache.sqoop.testutil.HsqldbTestServer;
import org.apache.sqoop.testutil.ImportJobTestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 * Test that --target-dir works.
 */
public class TestTargetDir extends ImportJobTestCase {

  public static final Log LOG = LogFactory
      .getLog(TestTargetDir.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected ArrayList getOutputArgv(boolean includeHadoopFlags) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(HsqldbTestServer.getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--as-sequencefile");

    return args;
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }

  /** test invalid argument exception if several output options. */
  @Test
  public void testSeveralOutputsIOException() throws IOException {
    ArrayList args = getOutputArgv(true);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--target-dir");
    args.add(getWarehouseDir());

    String[] argv = (String[]) args.toArray(new String[0]);

    thrown.expect(IOException.class);
    thrown.reportMissingExceptionWithMessage("Expected IOException on several output options");
    runImport(argv);
  }

  /** test target-dir contains imported files. */
  @Test
  public void testTargetDir() throws IOException {

    try {
      String targetDir = getWarehouseDir() + "/tempTargetDir";

      ArrayList args = getOutputArgv(true);
      args.add("--target-dir");
      args.add(targetDir);

      // delete target-dir if exists and recreate it
      FileSystem fs = FileSystem.get(getConf());
      Path outputPath = new Path(targetDir);
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }

      String[] argv = (String[]) args.toArray(new String[0]);
      runImport(argv);

      ContentSummary summ = fs.getContentSummary(outputPath);

      assertTrue("There's no new imported files in target-dir",
          summ.getFileCount() > 0);

    } catch (Exception e) {
      LOG.error("Got Exception: " + StringUtils.stringifyException(e));
      fail(e.toString());
    }
  }

  /** test target-dir breaks if already existing
   * (only allowed in append mode). */
  @Test
  public void testExistingTargetDir() throws IOException {
    String targetDir = getWarehouseDir() + "/tempTargetDir";

    ArrayList args = getOutputArgv(true);
    args.add("--target-dir");
    args.add(targetDir);

    // delete target-dir if exists and recreate it
    FileSystem fs = FileSystem.get(getConf());
    Path outputPath = new Path(targetDir);
    if (!fs.exists(outputPath)) {
      fs.mkdirs(outputPath);
    }

    String[] argv = (String[]) args.toArray(new String[0]);

    thrown.expect(IOException.class);
    thrown.reportMissingExceptionWithMessage("Expected IOException on --target-dir if target dir already exists");
    runImport(argv);
  }

}
