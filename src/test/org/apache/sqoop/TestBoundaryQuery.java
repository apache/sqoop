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

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.SqoopOptions.InvalidOptionsException;
import org.apache.sqoop.orm.CompilationManager;
import org.apache.sqoop.testutil.CommonArgs;
import org.apache.sqoop.testutil.HsqldbTestServer;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.testutil.SeqFileReader;
import org.apache.sqoop.tool.ImportTool;
import org.apache.sqoop.util.ClassLoaderStack;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that --boundary-query works in Sqoop.
 */
public class TestBoundaryQuery extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags, boolean tableImport,
      String boundaryQuery,
      String targetDir, String... extraArgs) {

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    if (tableImport) {
      args.add("--table");
      args.add(HsqldbTestServer.getTableName());
    } else {
      args.add("--query");
      args.add("SELECT INTFIELD1, INTFIELD2 FROM "
            + HsqldbTestServer.getTableName() + " WHERE $CONDITIONS");
    }
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    if (boundaryQuery != null) {
      args.add("--boundary-query");
      args.add(boundaryQuery);
    }
    args.add("--as-sequencefile");
    args.add("--target-dir");
    args.add(targetDir);
    args.add("--class-name");
    args.add(getTableName());
    args.add("--verbose");
    for (String extraArg : extraArgs) {
      args.add(extraArg);
    }

    return args.toArray(new String[0]);
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }


  /**
   * Given a comma-delimited list of integers, grab and parse the first int.
   * @param str a comma-delimited list of values, the first of which is an int.
   * @return the first field in the string, cast to int
   */
  private int getFirstInt(String str) {
    String [] parts = str.split(",");
    return Integer.parseInt(parts[0]);
  }

  public void runQueryTest(String query, boolean tableImport,
      int numExpectedResults, int expectedSum, String targetDir,
      String... extraArgs) throws IOException {

    ClassLoader prevClassLoader = null;
    SequenceFile.Reader reader = null;

    String [] argv = getArgv(true, tableImport, query, targetDir, extraArgs);
    runImport(argv);
    try {
      SqoopOptions opts = new ImportTool().parseArguments(
          getArgv(false, tableImport, query, targetDir, extraArgs),
          null, null, true);

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();

      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
          getTableName());

      reader = SeqFileReader.getSeqFileReader(getDataFilePath().toString());

      // here we can actually instantiate (k, v) pairs.
      Configuration conf = new Configuration();
      Object key = ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Object val = ReflectionUtils.newInstance(reader.getValueClass(), conf);

      if (reader.next(key) == null) {
        fail("Empty SequenceFile during import");
      }

      // make sure that the value we think should be at the top, is.
      reader.getCurrentValue(val);

      // We know that these values are two ints separated by a ',' character.
      // Since this is all dynamic, though, we don't want to actually link
      // against the class and use its methods. So we just parse this back
      // into int fields manually.  Sum them up and ensure that we get the
      // expected total for the first column, to verify that we got all the
      // results from the db into the file.
      int curSum = getFirstInt(val.toString());
      int totalResults = 1;

      // now sum up everything else in the file.
      while (reader.next(key) != null) {
        reader.getCurrentValue(val);
        curSum += getFirstInt(val.toString());
        totalResults++;
      }

      assertEquals("Total sum of first db column mismatch", expectedSum,
          curSum);
      assertEquals("Incorrect number of results for query", numExpectedResults,
          totalResults);
    } catch (InvalidOptionsException ioe) {
      fail(ioe.toString());
    } catch (ParseException pe) {
      fail(pe.toString());
    } finally {
      IOUtils.closeStream(reader);

      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  @Test
  public void testBoundaryQuery() throws IOException {
    System.out.println("PCYO");
    String query = "select min(intfield1), max(intfield1) from "
      + getTableName() +" where intfield1 in (3, 5)";

    runQueryTest(query, true, 2, 8, getTablePath().toString(),
      "--m", "1", "--split-by", "INTFIELD1");
  }

  @Test
  public void testNoBoundaryQuerySingleMapper() throws IOException {

      runQueryTest(null, false, 4, 16, getTablePath().toString(),
          "--m", "1");
  }
}
