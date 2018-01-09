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
 * Test that --split-by works.
 */
public class TestSplitBy extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags, String [] colNames,
      String splitByCol) {
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(HsqldbTestServer.getTableName());
    args.add("--columns");
    args.add(columnsString);
    args.add("--split-by");
    args.add(splitByCol);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("1");

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

  public void runSplitByTest(String splitByCol, int expectedSum)
      throws IOException {

    String [] columns = HsqldbTestServer.getFieldNames();
    ClassLoader prevClassLoader = null;
    SequenceFile.Reader reader = null;

    String [] argv = getArgv(true, columns, splitByCol);
    runImport(argv);
    try {
      SqoopOptions opts = new ImportTool().parseArguments(
          getArgv(false, columns, splitByCol),
          null, null, true);

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();
      LOG.debug("Got jar from import job: " + jarFileName);

      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName,
          getTableName());

      reader = SeqFileReader.getSeqFileReader(getDataFilePath().toString());

      // here we can actually instantiate (k, v) pairs.
      Configuration conf = new Configuration();
      Object key = ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Object val = ReflectionUtils.newInstance(reader.getValueClass(), conf);

      // We know that these values are two ints separated by a ',' character.
      // Since this is all dynamic, though, we don't want to actually link
      // against the class and use its methods. So we just parse this back
      // into int fields manually.  Sum them up and ensure that we get the
      // expected total for the first column, to verify that we got all the
      // results from the db into the file.

      // Sum up everything in the file.
      int curSum = 0;
      while (reader.next(key) != null) {
        reader.getCurrentValue(val);
        curSum += getFirstInt(val.toString());
      }

      assertEquals("Total sum of first db column mismatch", expectedSum,
          curSum);
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
  public void testSplitByFirstCol() throws IOException {
    String splitByCol = "INTFIELD1";
    runSplitByTest(splitByCol, HsqldbTestServer.getFirstColSum());
  }

  @Test
  public void testSplitBySecondCol() throws IOException {
    String splitByCol = "INTFIELD2";
    runSplitByTest(splitByCol, HsqldbTestServer.getFirstColSum());
  }
}
