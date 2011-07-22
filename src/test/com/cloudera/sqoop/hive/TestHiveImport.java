/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.hive;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.CodeGenTool;
import com.cloudera.sqoop.tool.CreateHiveTableTool;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * Test HiveImport capability after an import to HDFS.
 */
public class TestHiveImport extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      TestHiveImport.class.getName());

  /**
   * Sets the expected number of columns in the table being manipulated
   * by the test. Under the hood, this sets the expected column names
   * to DATA_COLi for 0 &lt;= i &lt; numCols.
   * @param numCols the number of columns to be created.
   */
  private void setNumCols(int numCols) {
    String [] cols = new String[numCols];
    for (int i = 0; i < numCols; i++) {
      cols[i] = "DATA_COL" + i;
    }

    setColNames(cols);
  }

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags, String [] moreArgs) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    if (null != moreArgs) {
      for (String arg: moreArgs) {
        args.add(arg);
      }
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--hive-import");
    String [] colNames = getColNames();
    if (null != colNames) {
      args.add("--split-by");
      args.add(colNames[0]);
    } else {
      fail("Could not determine column names.");
    }

    args.add("--num-mappers");
    args.add("1");

    for (String a : args) {
      LOG.debug("ARG : "+ a);
    }

    return args.toArray(new String[0]);
  }

  /**
   * @return the argv to supply to a code-gen only job for Hive imports.
   */
  protected String [] getCodeGenArgs() {
    ArrayList<String> args = new ArrayList<String>();

    args.add("--table");
    args.add(getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--hive-import");

    return args.toArray(new String[0]);
  }

  /**
   * @return the argv to supply to a ddl-executing-only job for Hive imports.
   */
  protected String [] getCreateHiveTableArgs(String [] extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    args.add("--table");
    args.add(getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());

    if (null != extraArgs) {
      for (String arg : extraArgs) {
        args.add(arg);
      }
    }

    return args.toArray(new String[0]);
  }

  private SqoopOptions getSqoopOptions(String [] args, SqoopTool tool) {
    SqoopOptions opts = null;
    try {
      opts = tool.parseArguments(args, null, null, true);
    } catch (Exception e) {
      fail("Invalid options: " + e.toString());
    }

    return opts;
  }

  private void runImportTest(String tableName, String [] types,
      String [] values, String verificationScript, String [] args,
      SqoopTool tool) throws IOException {

    // create a table and populate it with a row...
    createTableWithColTypes(types, values);
    
    // set up our mock hive shell to compare our generated script
    // against the correct expected one.
    SqoopOptions options = getSqoopOptions(args, tool);
    String hiveHome = options.getHiveHome();
    assertNotNull("hive.home was not set", hiveHome);
    Path testDataPath = new Path(new Path(hiveHome),
        "scripts/" + verificationScript);
    System.setProperty("expected.script", testDataPath.toString());

    // verify that we can import it correctly into hive.
    runImport(tool, args);
  }

  /** Test that we can generate a file containing the DDL and not import. */
  @Test
  public void testGenerateOnly() throws IOException {
    final String TABLE_NAME = "GenerateOnly";
    setCurTableName(TABLE_NAME);
    setNumCols(1);

    // Figure out where our target generated .q file is going to be.
    SqoopOptions options = getSqoopOptions(getArgv(false, null),
        new ImportTool());
    Path ddlFile = new Path(new Path(options.getCodeOutputDir()),
        TABLE_NAME + ".q");
    FileSystem fs = FileSystem.getLocal(new Configuration());

    // If it's already there, remove it before running the test to ensure
    // that it's the current test that generated the file.
    if (fs.exists(ddlFile)) {
      if (!fs.delete(ddlFile, false)) {
        LOG.warn("Could not delete previous ddl file: " + ddlFile);
      }
    }

    // Run a basic import, but specify that we're just generating definitions.
    String [] types = { "INTEGER" };
    String [] vals = { "42" };
    runImportTest(TABLE_NAME, types, vals, null, getCodeGenArgs(),
        new CodeGenTool());

    // Test that the generated definition file exists.
    assertTrue("Couldn't find expected ddl file", fs.exists(ddlFile));

    Path hiveImportPath = new Path(new Path(options.getWarehouseDir()),
        TABLE_NAME);
    assertFalse("Import actually happened!", fs.exists(hiveImportPath));
  }


  /** Test that strings and ints are handled in the normal fashion. */
  @Test
  public void testNormalHiveImport() throws IOException {
    final String TABLE_NAME = "NORMAL_HIVE_IMPORT";
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    String [] vals = { "'test'", "42", "'somestring'" };
    runImportTest(TABLE_NAME, types, vals, "normalImport.q",
        getArgv(false, null), new ImportTool());
  }

  /** Test that table is created in hive with no data import. */
  @Test
  public void testCreateOnlyHiveImport() throws IOException {
    final String TABLE_NAME = "CREATE_ONLY_HIVE_IMPORT";
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    String [] vals = { "'test'", "42", "'somestring'" };
    runImportTest(TABLE_NAME, types, vals,
        "createOnlyImport.q", getCreateHiveTableArgs(null),
        new CreateHiveTableTool());
  }

  /**
   * Test that table is created in hive and replaces the existing table if
   * any.
   */
  @Test
  public void testCreateOverwriteHiveImport() throws IOException {
    final String TABLE_NAME = "CREATE_OVERWRITE_HIVE_IMPORT";
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    String [] vals = { "'test'", "42", "'somestring'" };
    String [] extraArgs = {"--hive-overwrite"};
    runImportTest(TABLE_NAME, types, vals,
        "createOverwriteImport.q", getCreateHiveTableArgs(extraArgs),
        new CreateHiveTableTool());
  }

  /** Test that dates are coerced properly to strings. */
  @Test
  public void testDate() throws IOException {
    final String TABLE_NAME = "DATE_HIVE_IMPORT";
    setCurTableName(TABLE_NAME);
    setNumCols(2);
    String [] types = { "VARCHAR(32)", "DATE" };
    String [] vals = { "'test'", "'2009-05-12'" };
    runImportTest(TABLE_NAME, types, vals, "dateImport.q",
        getArgv(false, null), new ImportTool());
  }

  /** Test that NUMERICs are coerced to doubles. */
  @Test
  public void testNumeric() throws IOException {
    final String TABLE_NAME = "NUMERIC_HIVE_IMPORT";
    setCurTableName(TABLE_NAME);
    setNumCols(2);
    String [] types = { "NUMERIC", "CHAR(64)" };
    String [] vals = { "3.14159", "'foo'" };
    runImportTest(TABLE_NAME, types, vals, "numericImport.q",
        getArgv(false, null), new ImportTool());
  }

  /** If bin/hive returns an error exit status, we should get an IOException. */
  @Test
  public void testHiveExitFails() {
    // The expected script is different than the one which would be generated
    // by this, so we expect an IOException out.
    final String TABLE_NAME = "FAILING_HIVE_IMPORT";
    setCurTableName(TABLE_NAME);
    setNumCols(2);
    String [] types = { "NUMERIC", "CHAR(64)" };
    String [] vals = { "3.14159", "'foo'" };
    try {
      runImportTest(TABLE_NAME, types, vals, "failingImport.q",
          getArgv(false, null), new ImportTool());
      // If we get here, then the run succeeded -- which is incorrect.
      fail("FAILING_HIVE_IMPORT test should have thrown IOException");
    } catch (IOException ioe) {
      // expected; ok.
    }
  }

  /** Test that we can set delimiters how we want them. */
  @Test
  public void testCustomDelimiters() throws IOException {
    final String TABLE_NAME = "CUSTOM_DELIM_IMPORT";
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    String [] vals = { "'test'", "42", "'somestring'" };
    String [] extraArgs = {
      "--fields-terminated-by", ",",
      "--lines-terminated-by", "|",
    };
    runImportTest(TABLE_NAME, types, vals, "customDelimImport.q",
        getArgv(false, extraArgs), new ImportTool());
  }
}

