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

package com.cloudera.sqoop.hive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.BaseSqoopTool;
import com.cloudera.sqoop.tool.CodeGenTool;
import com.cloudera.sqoop.tool.CreateHiveTableTool;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;
import org.apache.commons.cli.ParseException;

/**
 * Test HiveImport capability after an import to HDFS.
 */
public class TestHiveImport extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      TestHiveImport.class.getName());

  public void setUp() {
    super.setUp();
    HiveImport.setTestMode(true);
  }

  public void tearDown() {
    super.tearDown();
    HiveImport.setTestMode(false);
  }

  /**
   * Sets the expected number of columns in the table being manipulated
   * by the test. Under the hood, this sets the expected column names
   * to DATA_COLi for 0 &lt;= i &lt; numCols.
   * @param numCols the number of columns to be created.
   */
  protected void setNumCols(int numCols) {
    String [] cols = new String[numCols];
    for (int i = 0; i < numCols; i++) {
      cols[i] = "DATA_COL" + i;
    }

    setColNames(cols);
  }

  protected String[] getTypesNewLineTest() {
    String[] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    return types;
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
    args.add(getConnectString());
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
   * @return the argv to supply to a create-table only job for Hive imports.
   */
  protected String [] getCreateTableArgv(boolean includeHadoopFlags,
      String [] moreArgs) {

    ArrayList<String> args = new ArrayList<String>();

    if (null != moreArgs) {
      for (String arg: moreArgs) {
        args.add(arg);
      }
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--connect");
    args.add(getConnectString());

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
    args.add(getConnectString());
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
    args.add(getConnectString());

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
    String testDataPath = new Path(new Path(hiveHome),
        "scripts/" + verificationScript).toString();
    System.setProperty("expected.script",
        new File(testDataPath).getAbsolutePath());

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

  /** Test that strings and ints are handled in the normal fashion as parquet
   * file. */
  @Test
  public void testNormalHiveImportAsParquet() throws IOException {
    final String TABLE_NAME = "NORMAL_HIVE_IMPORT_AS_PARQUET";
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    String [] vals = { "'test'", "42", "'somestring'" };
    String [] args_array = getArgv(false, null);
    ArrayList<String> args = new ArrayList<String>(Arrays.asList(args_array));
    args.add("--as-parquetfile");
    runImportTest(TABLE_NAME, types, vals, "normalImportAsParquet.q", args.toArray(new String[0]),
            new ImportTool());
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
    String [] extraArgs = {"--hive-overwrite", "--create-hive-table"};
    runImportTest(TABLE_NAME, types, vals,
        "createOverwriteImport.q", getCreateHiveTableArgs(extraArgs),
        new CreateHiveTableTool());
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

  /**
   * Test hive import with row that has new line in it.
   */
  @Test
  public void testFieldWithHiveDelims() throws IOException,
    InterruptedException {
    final String TABLE_NAME = "FIELD_WITH_NL_HIVE_IMPORT";

    LOG.info("Doing import of single row into FIELD_WITH_NL_HIVE_IMPORT table");
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String[] types = getTypesNewLineTest();
    String[] vals = { "'test with \n new lines \n'", "42",
        "'oh no " + '\01' + " field delims " + '\01' + "'", };
    String[] moreArgs = { "--"+ BaseSqoopTool.HIVE_DROP_DELIMS_ARG };

    runImportTest(TABLE_NAME, types, vals, "fieldWithNewlineImport.q",
        getArgv(false, moreArgs), new ImportTool());

    LOG.info("Validating data in single row is present in: "
          + "FIELD_WITH_NL_HIVE_IMPORT table");

    // Ideally, we would actually invoke hive code to verify that record with
    // record and field delimiters have values replaced and that we have the
    // proper number of hive records. Unfortunately, this is a non-trivial task,
    // and better dealt with at an integration test level
    //
    // Instead, this assumes the path of the generated table and just validate
    // map job output.

    // Get and read the raw output file
    String whDir = getWarehouseDir();
    File p = new File(new File(whDir, TABLE_NAME), "part-m-00000");
    File f = new File(p.toString());
    FileReader fr = new FileReader(f);
    BufferedReader br = new BufferedReader(fr);
    try {
      // verify the output
      assertEquals(br.readLine(), "test with  new lines " + '\01' + "42"
          + '\01' + "oh no  field delims ");
      assertEquals(br.readLine(), null); // should only be one line
    } catch (IOException ioe) {
      fail("Unable to read files generated from hive");
    } finally {
      br.close();
    }
  }

  /**
   * Test hive import with row that has new line in it.
   */
  @Test
  public void testFieldWithHiveDelimsReplacement() throws IOException,
    InterruptedException {
    final String TABLE_NAME = "FIELD_WITH_NL_REPLACEMENT_HIVE_IMPORT";

    LOG.info("Doing import of single row into "
        + "FIELD_WITH_NL_REPLACEMENT_HIVE_IMPORT table");
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String[] types = getTypesNewLineTest();
    String[] vals = { "'test with\nnew lines\n'", "42",
        "'oh no " + '\01' + " field delims " + '\01' + "'", };
    String[] moreArgs = { "--"+BaseSqoopTool.HIVE_DELIMS_REPLACEMENT_ARG, " "};

    runImportTest(TABLE_NAME, types, vals,
        "fieldWithNewlineReplacementImport.q", getArgv(false, moreArgs),
        new ImportTool());

    LOG.info("Validating data in single row is present in: "
          + "FIELD_WITH_NL_REPLACEMENT_HIVE_IMPORT table");

    // Ideally, we would actually invoke hive code to verify that record with
    // record and field delimiters have values replaced and that we have the
    // proper number of hive records. Unfortunately, this is a non-trivial task,
    // and better dealt with at an integration test level
    //
    // Instead, this assumes the path of the generated table and just validate
    // map job output.

    // Get and read the raw output file
    String whDir = getWarehouseDir();
    File p = new File(new File(whDir, TABLE_NAME), "part-m-00000");
    File f = new File(p.toString());
    FileReader fr = new FileReader(f);
    BufferedReader br = new BufferedReader(fr);
    try {
      // verify the output
      assertEquals(br.readLine(), "test with new lines " + '\01' + "42"
          + '\01' + "oh no   field delims  ");
      assertEquals(br.readLine(), null); // should only be one line
    } catch (IOException ioe) {
      fail("Unable to read files generated from hive");
    } finally {
      br.close();
    }
  }

  /**
   * Test hive drop and replace option validation.
   */
  @Test
  public void testHiveDropAndReplaceOptionValidation() throws ParseException {
    LOG.info("Testing conflicting Hive delimiter drop/replace options");

    setNumCols(3);
    String[] moreArgs = { "--"+BaseSqoopTool.HIVE_DELIMS_REPLACEMENT_ARG, " ",
      "--"+BaseSqoopTool.HIVE_DROP_DELIMS_ARG, };

    ImportTool tool = new ImportTool();
    try {
      tool.validateOptions(tool.parseArguments(getArgv(false, moreArgs), null,
          null, true));
      fail("Expected InvalidOptionsException");
    } catch (InvalidOptionsException ex) {
      /* success */
    }
  }

  /**
   * Test hive import with row that has new line in it.
   */
  @Test
  public void testImportHiveWithPartitions() throws IOException,
      InterruptedException {
    final String TABLE_NAME = "PARTITION_HIVE_IMPORT";

    LOG.info("Doing import of single row into PARTITION_HIVE_IMPORT table");
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String[] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)", };
    String[] vals = { "'whoop'", "42", "'I am a row in a partition'", };
    String[] moreArgs = { "--" + BaseSqoopTool.HIVE_PARTITION_KEY_ARG, "ds",
        "--" + BaseSqoopTool.HIVE_PARTITION_VALUE_ARG, "20110413", };

    runImportTest(TABLE_NAME, types, vals, "partitionImport.q",
        getArgv(false, moreArgs), new ImportTool());
  }

  /**
   * If partition key is set to one of importing columns, we should get an
   * IOException.
   * */
  @Test
  public void testImportWithBadPartitionKey() {
    final String TABLE_NAME = "FAILING_PARTITION_HIVE_IMPORT";

    LOG.info("Doing import of single row into " + TABLE_NAME + " table");
    setCurTableName(TABLE_NAME);
    setNumCols(3);
    String[] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)", };
    String[] vals = { "'key'", "42", "'I am a row in a partition'", };

    String partitionKey = getColNames()[0];

    // Specify 1st column as partition key and import every column of the
    // table by default (i.e. no --columns option).
    String[] moreArgs1 = {
        "--" + BaseSqoopTool.HIVE_PARTITION_KEY_ARG,
        partitionKey,
    };

    // Specify 1st column as both partition key and importing column.
    String[] moreArgs2 = {
        "--" + BaseSqoopTool.HIVE_PARTITION_KEY_ARG,
        partitionKey,
        "--" + BaseSqoopTool.COLUMNS_ARG,
        partitionKey,
    };

    // Test hive-import with the 1st args.
    try {
      runImportTest(TABLE_NAME, types, vals, "partitionImport.q",
          getArgv(false, moreArgs1), new ImportTool());
      fail(TABLE_NAME + " test should have thrown IOException");
    } catch (IOException ioe) {
      // expected; ok.
    }

    // Test hive-import with the 2nd args.
    try {
      runImportTest(TABLE_NAME, types, vals, "partitionImport.q",
          getArgv(false, moreArgs2), new ImportTool());
      fail(TABLE_NAME + " test should have thrown IOException");
    } catch (IOException ioe) {
      // expected; ok.
    }

    // Test create-hive-table with the 1st args.
    try {
      runImportTest(TABLE_NAME, types, vals, "partitionImport.q",
          getCreateTableArgv(false, moreArgs1), new CreateHiveTableTool());
      fail(TABLE_NAME + " test should have thrown IOException");
    } catch (IOException ioe) {
      // expected; ok.
    }
  }
}
