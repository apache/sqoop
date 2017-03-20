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

package com.cloudera.sqoop;

import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.apache.sqoop.manager.oracle.OracleUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.tool.BaseSqoopTool;
import com.cloudera.sqoop.tool.ImportTool;

import static org.apache.sqoop.Sqoop.SQOOP_RETHROW_PROPERTY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test aspects of the SqoopOptions class.
 */
public class TestSqoopOptions {

  private Properties originalSystemProperties;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
   originalSystemProperties = System.getProperties();
  }

  @After
  public void tearDown() {
    System.setProperties(originalSystemProperties);
  }

  // tests for the toChar() parser
  @Test
  public void testNormalChar() throws Exception {
    assertEquals('a', SqoopOptions.toChar("a"));
  }

  @Test
  public void testEmptyString() throws Exception {
    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on empty string");
    SqoopOptions.toChar("");
  }

  @Test
  public void testNullString() throws Exception {
    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on null string");
    SqoopOptions.toChar(null);
  }

  @Test
  public void testTooLong() throws Exception {
    // Should just use the first character and log a warning.
    assertEquals('x', SqoopOptions.toChar("xyz"));
  }

  @Test
  public void testHexChar1() throws Exception {
    assertEquals(0xF, SqoopOptions.toChar("\\0xf"));
  }

  @Test
  public void testHexChar2() throws Exception {
    assertEquals(0xF, SqoopOptions.toChar("\\0xF"));
  }

  @Test
  public void testHexChar3() throws Exception {
    assertEquals(0xF0, SqoopOptions.toChar("\\0xf0"));
  }

  @Test
  public void testHexChar4() throws Exception {
    assertEquals(0xF0, SqoopOptions.toChar("\\0Xf0"));
  }

  @Test
  public void testEscapeChar1() throws Exception {
    assertEquals('\n', SqoopOptions.toChar("\\n"));
  }

  @Test
  public void testEscapeChar2() throws Exception {
    assertEquals('\\', SqoopOptions.toChar("\\\\"));
  }

  @Test
  public void testEscapeChar3() throws Exception {
    assertEquals('\\', SqoopOptions.toChar("\\"));
  }

  @Test
  public void testWhitespaceToChar() throws Exception {
    assertEquals(' ', SqoopOptions.toChar(" "));
    assertEquals(' ', SqoopOptions.toChar("   "));
    assertEquals('\t', SqoopOptions.toChar("\t"));
  }

  @Test
  public void testUnknownEscape1() throws Exception {
    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on unknown escaping");
    SqoopOptions.toChar("\\Q");
  }

  @Test
  public void testUnknownEscape2() throws Exception {
    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on unknown escaping");
    SqoopOptions.toChar("\\nn");
  }

  @Test
  public void testEscapeNul1() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\0"));
  }

  @Test
  public void testEscapeNul2() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\00"));
  }

  @Test
  public void testEscapeNul3() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\0000"));
  }

  @Test
  public void testEscapeNul4() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\0x0"));
  }

  @Test
  public void testOctalChar1() throws Exception {
    assertEquals(04, SqoopOptions.toChar("\\04"));
  }

  @Test
  public void testOctalChar2() throws Exception {
    assertEquals(045, SqoopOptions.toChar("\\045"));
  }

  @Test
  public void testErrOctalChar() throws Exception {
    thrown.expect(NumberFormatException.class);
    thrown.reportMissingExceptionWithMessage("Expected NumberFormatException on erroneous octal char");
    SqoopOptions.toChar("\\095");
  }

  @Test
  public void testErrHexChar() throws Exception {
    thrown.expect(NumberFormatException.class);
    thrown.reportMissingExceptionWithMessage("Expected NumberFormatException on erroneous hex char");
    SqoopOptions.toChar("\\0x9K5");
  }

  private SqoopOptions parse(String [] argv) throws Exception {
    ImportTool importTool = new ImportTool();
    return importTool.parseArguments(argv, null, null, false);
  }

  // test that setting output delimiters also sets input delimiters
  @Test
  public void testDelimitersInherit() throws Exception {
    String [] args = {
      "--fields-terminated-by",
      "|",
    };

    SqoopOptions opts = parse(args);
    assertEquals('|', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  // Test that setting output delimiters and setting input delims
  // separately works.
  @Test
  public void testDelimOverride1() throws Exception {
    String [] args = {
      "--fields-terminated-by",
      "|",
      "--input-fields-terminated-by",
      "*",
    };

    SqoopOptions opts = parse(args);
    assertEquals('*', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  // test that the order in which delims are specified doesn't matter
  @Test
  public void testDelimOverride2() throws Exception {
    String [] args = {
      "--input-fields-terminated-by",
      "*",
      "--fields-terminated-by",
      "|",
    };

    SqoopOptions opts = parse(args);
    assertEquals('*', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  @Test
  public void testBadNumMappers1() throws Exception {
    String [] args = {
      "--num-mappers",
      "x",
    };

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on invalid --num-mappers argument");
    parse(args);
  }

  @Test
  public void testBadNumMappers2() throws Exception {
    String [] args = {
      "-m",
      "x",
    };

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on invalid -m argument");
    parse(args);
  }

  @Test
  public void testGoodNumMappers() throws Exception {
    String [] args = {
      "-m",
      "4",
    };

    SqoopOptions opts = parse(args);
    assertEquals(4, opts.getNumMappers());
  }

  @Test
  public void testHivePartitionParams() throws Exception {
    String[] args = {
        "--hive-partition-key", "ds",
        "--hive-partition-value", "20110413",
    };
    SqoopOptions opts = parse(args);
    assertEquals("ds", opts.getHivePartitionKey());
    assertEquals("20110413", opts.getHivePartitionValue());
  }

  @Test
  public void testBoundaryQueryParams() throws Exception {
    String[] args = {
      "--boundary-query", "select 1, 2",
    };

    SqoopOptions opts = parse(args);
    assertEquals("select 1, 2", opts.getBoundaryQuery());
  }

  @Test
  public void testMapColumnHiveParams() throws Exception {
    String[] args = {
      "--map-column-hive", "id=STRING",
    };

    SqoopOptions opts = parse(args);
    Properties mapping = opts.getMapColumnHive();
    assertTrue(mapping.containsKey("id"));
    assertEquals("STRING", mapping.get("id"));
  }

  @Test
  public void testMalformedMapColumnHiveParams() throws Exception {
    String[] args = {
      "--map-column-hive", "id",
    };
    try {
      SqoopOptions opts = parse(args);
      fail("Malformed hive mapping does not throw exception");
    } catch (Exception e) {
      // Caught exception as expected
    }
  }

  @Test
  public void testMapColumnJavaParams() throws Exception {
    String[] args = {
      "--map-column-java", "id=String",
    };

    SqoopOptions opts = parse(args);
    Properties mapping = opts.getMapColumnJava();
    assertTrue(mapping.containsKey("id"));
    assertEquals("String", mapping.get("id"));
  }

  @Test
  public void testMalfromedMapColumnJavaParams() throws Exception {
    String[] args = {
      "--map-column-java", "id",
    };
    try {
      SqoopOptions opts = parse(args);
      fail("Malformed java mapping does not throw exception");
    } catch (Exception e) {
      // Caught exception as expected
    }
  }

  @Test
  public void testSkipDistCacheOption() throws Exception {
    String[] args = {"--skip-dist-cache"};
    SqoopOptions opts = parse(args);
    assertTrue(opts.isSkipDistCache());
  }

  @Test
  public void testPropertySerialization1() {
    // Test that if we write a SqoopOptions out to a Properties,
    // and then read it back in, we get all the same results.
    SqoopOptions out = new SqoopOptions();
    out.setUsername("user");
    out.setConnectString("bla");
    out.setNumMappers(4);
    out.setAppendMode(true);
    out.setHBaseTable("hbasetable");
    out.setWarehouseDir("Warehouse");
    out.setClassName("someclass");
    out.setSplitByCol("somecol");
    out.setSqlQuery("the query");
    out.setPackageName("a.package");
    out.setHiveImport(true);
    out.setFetchSize(null);

    Properties connParams = new Properties();
    connParams.put("conn.timeout", "3000");
    connParams.put("conn.buffer_size", "256");
    connParams.put("conn.dummy", "dummy");
    connParams.put("conn.foo", "bar");

    out.setConnectionParams(connParams);

    Properties outProps = out.writeProperties();

    SqoopOptions in = new SqoopOptions();
    in.loadProperties(outProps);

    Properties inProps = in.writeProperties();

    assertEquals("properties don't match", outProps, inProps);

    assertEquals("connection params don't match",
            connParams, out.getConnectionParams());
    assertEquals("connection params don't match",
            connParams, in.getConnectionParams());
  }

  @Test
  public void testPropertySerialization2() {
    // Test that if we write a SqoopOptions out to a Properties,
    // and then read it back in, we get all the same results.
    SqoopOptions out = new SqoopOptions();
    out.setUsername("user");
    out.setConnectString("bla");
    out.setNumMappers(4);
    out.setAppendMode(true);
    out.setHBaseTable("hbasetable");
    out.setWarehouseDir("Warehouse");
    out.setClassName("someclass");
    out.setSplitByCol("somecol");
    out.setSqlQuery("the query");
    out.setPackageName("a.package");
    out.setHiveImport(true);
    out.setFetchSize(42);

    Properties connParams = new Properties();
    connParams.setProperty("a", "value-a");
    connParams.setProperty("b", "value-b");
    connParams.setProperty("a.b", "value-a.b");
    connParams.setProperty("a.b.c", "value-a.b.c");
    connParams.setProperty("aaaaaaaaaa.bbbbbbb.cccccccc", "value-abc");

    out.setConnectionParams(connParams);

    Properties outProps = out.writeProperties();

    SqoopOptions in = new SqoopOptions();
    in.loadProperties(outProps);

    Properties inProps = in.writeProperties();

    assertEquals("properties don't match", outProps, inProps);
    assertEquals("connection params don't match",
            connParams, out.getConnectionParams());
    assertEquals("connection params don't match",
            connParams, in.getConnectionParams());
  }

  @Test
  public void testDefaultTempRootDir() {
    SqoopOptions opts = new SqoopOptions();

    assertEquals("_sqoop", opts.getTempRootDir());
  }

  @Test
  public void testDefaultLoadedTempRootDir() {
    SqoopOptions out = new SqoopOptions();
    Properties props = out.writeProperties();
    SqoopOptions opts = new SqoopOptions();
    opts.loadProperties(props);

    assertEquals("_sqoop", opts.getTempRootDir());
  }

  @Test
  public void testLoadedTempRootDir() {
    SqoopOptions out = new SqoopOptions();
    final String tempRootDir = "customRoot";
    out.setTempRootDir(tempRootDir);
    Properties props = out.writeProperties();
    SqoopOptions opts = new SqoopOptions();
    opts.loadProperties(props);

    assertEquals(tempRootDir, opts.getTempRootDir());
  }

  @Test
  public void testNulledTempRootDir() {
    SqoopOptions out = new SqoopOptions();
    out.setTempRootDir(null);
    Properties props = out.writeProperties();
    SqoopOptions opts = new SqoopOptions();
    opts.loadProperties(props);

    assertEquals("_sqoop", opts.getTempRootDir());
  }

  @Test
  public void testDefaultThrowOnErrorWithNotSetSystemProperty() {
    System.clearProperty(SQOOP_RETHROW_PROPERTY);
    SqoopOptions opts = new SqoopOptions();
    assertFalse(opts.isThrowOnError());
  }

  @Test
  public void testDefaultThrowOnErrorWithSetSystemProperty() {
    String testSqoopRethrowProperty = "";
    System.setProperty(SQOOP_RETHROW_PROPERTY, testSqoopRethrowProperty);
    SqoopOptions opts = new SqoopOptions();

    assertTrue(opts.isThrowOnError());
  }

  @Test
  public void testDefaultLoadedThrowOnErrorWithNotSetSystemProperty() {
    System.clearProperty(SQOOP_RETHROW_PROPERTY);
    SqoopOptions out = new SqoopOptions();
    Properties props = out.writeProperties();
    SqoopOptions opts = new SqoopOptions();
    opts.loadProperties(props);

    assertFalse(opts.isThrowOnError());
  }

  @Test
  public void testDefaultLoadedThrowOnErrorWithSetSystemProperty() {
    String testSqoopRethrowProperty = "";
    System.setProperty(SQOOP_RETHROW_PROPERTY, testSqoopRethrowProperty);
    SqoopOptions out = new SqoopOptions();
    Properties props = out.writeProperties();
    SqoopOptions opts = new SqoopOptions();
    opts.loadProperties(props);

    assertTrue(opts.isThrowOnError());
  }

  @Test
  public void testThrowOnErrorWithNotSetSystemProperty() throws Exception {
    System.clearProperty(SQOOP_RETHROW_PROPERTY);
    String[] args = {"--throw-on-error"};
    SqoopOptions opts = parse(args);

    assertTrue(opts.isThrowOnError());
  }

  @Test
  public void testThrowOnErrorWithSetSystemProperty() throws Exception {
    String testSqoopRethrowProperty = "";
    System.setProperty(SQOOP_RETHROW_PROPERTY, testSqoopRethrowProperty);
    String[] args = {"--throw-on-error"};
    SqoopOptions opts = parse(args);

    assertTrue(opts.isThrowOnError());
  }

  @Test
  public void defaultValueOfOracleEscapingDisabledShouldBeFalse() {
    System.clearProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED);
    SqoopOptions opts = new SqoopOptions();

    assertThat(opts.isOracleEscapingDisabled(), is(equalTo(true)));
  }

  @Test
  public void valueOfOracleEscapingDisabledShouldBeFalseIfTheValueOfTheRelatedEnvironmentVariableIsSetToFalse() {
    System.setProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED, "false");
    SqoopOptions opts = new SqoopOptions();

    assertThat(opts.isOracleEscapingDisabled(), is(equalTo(false)));
  }

  @Test
  public void valueOfOracleEscapingDisabledShouldBeTrueIfTheValueOfTheRelatedEnvironmentVariableIsSetToTrue() {
    System.setProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED, "true");
    SqoopOptions opts = new SqoopOptions();

    assertThat(opts.isOracleEscapingDisabled(), is(equalTo(true)));
  }

  @Test
  public void valueOfOracleEscapingDisabledShouldBeFalseIfTheValueOfTheRelatedEnvironmentVariableIsSetToAnyNonBooleanValue() {
    System.setProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED, "falsetrue");
    SqoopOptions opts = new SqoopOptions();

    assertThat(opts.isOracleEscapingDisabled(), is(equalTo(false)));
  }

  @Test
  public void hadoopConfigurationInstanceOfSqoopOptionsShouldContainTheSameValueForOracleEscapingDisabledAsSqoopOptionsProperty() {
    SqoopOptions opts = new SqoopOptions();

    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(opts.isOracleEscapingDisabled())));
  }

  @Test
  public void hadoopConfigurationInstanceOfSqoopOptionsShouldContainTrueForOracleEscapingDisabledAsTheValueDirectlyHasBeenSetToSqoopOptions() {
    System.clearProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED);
    SqoopOptions opts = new SqoopOptions();
    opts.setOracleEscapingDisabled(true);

    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(true)));
  }

  @Test
  public void hadoopConfigurationInstanceOfSqoopOptionsShouldContainFalseForOracleEscapingDisabledAsTheValueDirectlyHasBeenSetToSqoopOptions() {
    System.clearProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED);
    SqoopOptions opts = new SqoopOptions();
    opts.setOracleEscapingDisabled(false);

    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(false)));
  }

  @Test
  public void valueOfOracleEscapingDisabledInHadoopConfigurationInstanceOfSqoopOptionsShouldBeFalseIfTheValueOfTheRelatedEnvironmentVariableIsSetToFalse() {
    System.setProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED, "false");
    SqoopOptions opts = new SqoopOptions();

    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(false)));
  }

  @Test
  public void valueOfOracleEscapingDisabledInHadoopConfigurationInstanceOfSqoopOptionsShouldBeTrueIfTheValueOfTheRelatedEnvironmentVariableIsSetToTrue() {
    System.setProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED, "true");
    SqoopOptions opts = new SqoopOptions();

    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(true)));
  }

  @Test
  public void valueOfOracleEscapingDisabledInHadoopConfigurationInstanceOfSqoopOptionsShouldBeFalseIfTheValueOfTheRelatedEnvironmentVariableIsSetToAnyNonBooleanValue() {
    System.setProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED, "falsetrue");
    SqoopOptions opts = new SqoopOptions();

    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(false)));
  }

  @Test
  public void valueOfOracleEscapingDisabledShouldBeAbleToSavedAndLoadedBackWithTheSameValue() {
    System.clearProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED);
    SqoopOptions opts = new SqoopOptions();
    opts.setOracleEscapingDisabled(false);
    Properties out = opts.writeProperties();
    opts = new SqoopOptions();
    opts.loadProperties(out);

    assertThat(opts.isOracleEscapingDisabled(), is(equalTo(false)));
    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(false)));
  }

  @Test
  public void valueOfOracleEscapingDisabledShouldBeEqualToNullIfASqoopOptionsInstanceWasLoadedWhichDidntContainASavedValueForIt() {
    System.clearProperty(SqoopOptions.ORACLE_ESCAPING_DISABLED);
    SqoopOptions opts = new SqoopOptions();
    Properties out = opts.writeProperties();
    opts = new SqoopOptions();
    opts.loadProperties(out);

    assertThat(opts.isOracleEscapingDisabled(), is(equalTo(true)));
    assertThat(OracleUtils.isOracleEscapingDisabled(opts.getConf()),
        is(equalTo(true)));
  }

  // test that hadoop-home is accepted as an option
  @Test
  public void testHadoopHome() throws Exception {
    String [] args = {
      "--hadoop-home",
      "/usr/lib/hadoop",
    };

    SqoopOptions opts = parse(args);
    assertEquals("/usr/lib/hadoop", opts.getHadoopMapRedHome());
  }

  // test that hadoop-home is accepted as an option
  @Test
  public void testHadoopMapRedOverridesHadoopHome() throws Exception {
    String[] args = { "--hadoop-home", "/usr/lib/hadoop-ignored", "--hadoop-mapred-home", "/usr/lib/hadoop", };

    SqoopOptions opts = parse(args);
    assertEquals("/usr/lib/hadoop", opts.getHadoopMapRedHome());
  }


  //helper method to validate given import options
  private void validateImportOptions(String[] extraArgs) throws Exception {
    String [] args = {
      "--connect", HsqldbTestServer.getUrl(),
      "--table", "test",
      "-m", "1",
    };
    ImportTool importTool = new ImportTool();
    SqoopOptions opts = importTool.parseArguments(
        (String []) ArrayUtils.addAll(args, extraArgs), null, null, false);
    importTool.validateOptions(opts);
  }

  //test compatability of --detele-target-dir with import
  @Test
  public void testDeteleTargetDir() throws Exception {
    String [] extraArgs = {
      "--delete-target-dir",
    };
    try {
      validateImportOptions(extraArgs);
    } catch(SqoopOptions.InvalidOptionsException ioe) {
      fail("Unexpected InvalidOptionsException" + ioe);
    }
  }

  //test incompatability of --delete-target-dir & --append with import
  @Test
  public void testDeleteTargetDirWithAppend() throws Exception {
    String [] extraArgs = {
      "--append",
      "--delete-target-dir",
    };

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on incompatibility of " +
        "--delete-target-dir and --append");
    validateImportOptions(extraArgs);
  }

  //test incompatability of --delete-target-dir with incremental import
  @Test
  public void testDeleteWithIncrementalImport() throws Exception {
    String [] extraArgs = {
      "--incremental", "append",
      "--delete-target-dir",
    };

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException on incompatibility of " +
        "--delete-target-dir and --incremental");
    validateImportOptions(extraArgs);
  }

  // test that hbase bulk load import with table name and target dir
  // passes validation
  @Test
  public void testHBaseBulkLoad() throws Exception {
    String [] extraArgs = {
        longArgument(BaseSqoopTool.HBASE_BULK_LOAD_ENABLED_ARG),
        longArgument(BaseSqoopTool.TARGET_DIR_ARG), "./test",
        longArgument(BaseSqoopTool.HBASE_TABLE_ARG), "test_table",
        longArgument(BaseSqoopTool.HBASE_COL_FAM_ARG), "d"};

    validateImportOptions(extraArgs);
  }

  // test that hbase bulk load import with a missing --hbase-table fails
  @Test
  public void testHBaseBulkLoadMissingHbaseTable() throws Exception {
    String [] extraArgs = {
        longArgument(BaseSqoopTool.HBASE_BULK_LOAD_ENABLED_ARG),
        longArgument(BaseSqoopTool.TARGET_DIR_ARG), "./test"};

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException because of missing --hbase-table");
    validateImportOptions(extraArgs);
  }

  private static String longArgument(String argument) {
    return String.format("--%s", argument);
  }

  @Test
  public void testRelaxedIsolation() throws Exception {
    String extraArgs[] = {
      "--relaxed-isolation",
    };
    validateImportOptions(extraArgs);
  }

  @Test
  public void testResetToOneMapper() throws Exception {
    String extraArgs[] = {
      "--autoreset-to-one-mapper",
    };
    validateImportOptions(extraArgs);
  }

  @Test
  public void testResetToOneMapperAndSplitBy() throws Exception {
    String extraArgs[] = {
      "--autoreset-to-one-mapper",
      "--split-by",
      "col0",
    };

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected Exception on incompatibility of " +
        "--autoreset-to-one-mapper and --split-by");
    validateImportOptions(extraArgs);
  }

  @Test
  public void testEscapeMapingColumnNames() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    // enabled by default
    assertTrue(opts.getEscapeMappingColumnNamesEnabled());

    String [] args = {
        "--" + org.apache.sqoop.tool.BaseSqoopTool.ESCAPE_MAPPING_COLUMN_NAMES_ENABLED,
        "false",
    };

    opts = parse(args);
    assertFalse(opts.getEscapeMappingColumnNamesEnabled());
  }

}
