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

import com.cloudera.sqoop.tool.BaseSqoopTool;
import junit.framework.TestCase;

import org.apache.commons.lang.ArrayUtils;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.testutil.HsqldbTestServer;

/**
 * Test aspects of the SqoopOptions class.
 */
public class TestSqoopOptions extends TestCase {

  // tests for the toChar() parser
  public void testNormalChar() throws Exception {
    assertEquals('a', SqoopOptions.toChar("a"));
  }

  public void testEmptyString() throws Exception {
    try {
      SqoopOptions.toChar("");
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testNullString() throws Exception {
    try {
      SqoopOptions.toChar(null);
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testTooLong() throws Exception {
    // Should just use the first character and log a warning.
    assertEquals('x', SqoopOptions.toChar("xyz"));
  }

  public void testHexChar1() throws Exception {
    assertEquals(0xF, SqoopOptions.toChar("\\0xf"));
  }

  public void testHexChar2() throws Exception {
    assertEquals(0xF, SqoopOptions.toChar("\\0xF"));
  }

  public void testHexChar3() throws Exception {
    assertEquals(0xF0, SqoopOptions.toChar("\\0xf0"));
  }

  public void testHexChar4() throws Exception {
    assertEquals(0xF0, SqoopOptions.toChar("\\0Xf0"));
  }

  public void testEscapeChar1() throws Exception {
    assertEquals('\n', SqoopOptions.toChar("\\n"));
  }

  public void testEscapeChar2() throws Exception {
    assertEquals('\\', SqoopOptions.toChar("\\\\"));
  }

  public void testEscapeChar3() throws Exception {
    assertEquals('\\', SqoopOptions.toChar("\\"));
  }

  public void testWhitespaceToChar() throws Exception {
    assertEquals(' ', SqoopOptions.toChar(" "));
    assertEquals(' ', SqoopOptions.toChar("   "));
    assertEquals('\t', SqoopOptions.toChar("\t"));
  }

  public void testUnknownEscape1() throws Exception {
    try {
      SqoopOptions.toChar("\\Q");
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testUnknownEscape2() throws Exception {
    try {
      SqoopOptions.toChar("\\nn");
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testEscapeNul1() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\0"));
  }

  public void testEscapeNul2() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\00"));
  }

  public void testEscapeNul3() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\0000"));
  }

  public void testEscapeNul4() throws Exception {
    assertEquals(DelimiterSet.NULL_CHAR, SqoopOptions.toChar("\\0x0"));
  }

  public void testOctalChar1() throws Exception {
    assertEquals(04, SqoopOptions.toChar("\\04"));
  }

  public void testOctalChar2() throws Exception {
    assertEquals(045, SqoopOptions.toChar("\\045"));
  }

  public void testErrOctalChar() throws Exception {
    try {
      SqoopOptions.toChar("\\095");
      fail("Expected exception");
    } catch (NumberFormatException nfe) {
      // expected.
    }
  }

  public void testErrHexChar() throws Exception {
    try {
      SqoopOptions.toChar("\\0x9K5");
      fail("Expected exception");
    } catch (NumberFormatException nfe) {
      // expected.
    }
  }

  private SqoopOptions parse(String [] argv) throws Exception {
    ImportTool importTool = new ImportTool();
    return importTool.parseArguments(argv, null, null, false);
  }

  // test that setting output delimiters also sets input delimiters
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

  public void testBadNumMappers1() throws Exception {
    String [] args = {
      "--num-mappers",
      "x",
    };

    try {
      parse(args);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testBadNumMappers2() throws Exception {
    String [] args = {
      "-m",
      "x",
    };

    try {
      parse(args);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testGoodNumMappers() throws Exception {
    String [] args = {
      "-m",
      "4",
    };

    SqoopOptions opts = parse(args);
    assertEquals(4, opts.getNumMappers());
  }

  public void testHivePartitionParams() throws Exception {
    String[] args = {
        "--hive-partition-key", "ds",
        "--hive-partition-value", "20110413",
    };
    SqoopOptions opts = parse(args);
    assertEquals("ds", opts.getHivePartitionKey());
    assertEquals("20110413", opts.getHivePartitionValue());
  }

  public void testBoundaryQueryParams() throws Exception {
    String[] args = {
      "--boundary-query", "select 1, 2",
    };

    SqoopOptions opts = parse(args);
    assertEquals("select 1, 2", opts.getBoundaryQuery());
  }

  public void testMapColumnHiveParams() throws Exception {
    String[] args = {
      "--map-column-hive", "id=STRING",
    };

    SqoopOptions opts = parse(args);
    Properties mapping = opts.getMapColumnHive();
    assertTrue(mapping.containsKey("id"));
    assertEquals("STRING", mapping.get("id"));
  }

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

  public void testMapColumnJavaParams() throws Exception {
    String[] args = {
      "--map-column-java", "id=String",
    };

    SqoopOptions opts = parse(args);
    Properties mapping = opts.getMapColumnJava();
    assertTrue(mapping.containsKey("id"));
    assertEquals("String", mapping.get("id"));
  }

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

  public void testSkipDistCacheOption() throws Exception {
    String[] args = {"--skip-dist-cache"};
    SqoopOptions opts = parse(args);
    assertTrue(opts.isSkipDistCache());
  }

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

  // test that hadoop-home is accepted as an option
  public void testHadoopHome() throws Exception {
    String [] args = {
      "--hadoop-home",
      "/usr/lib/hadoop",
    };

    SqoopOptions opts = parse(args);
    assertEquals("/usr/lib/hadoop", opts.getHadoopMapRedHome());
  }

  // test that hadoop-home is accepted as an option
  public void testHadoopMapRedOverridesHadoopHome() throws Exception {
	String[] args = { "--hadoop-home", "/usr/lib/hadoop-ignored",
	  "--hadoop-mapred-home", "/usr/lib/hadoop", };

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
  public void testDeleteTargetDirWithAppend() throws Exception {
    String [] extraArgs = {
      "--append",
      "--delete-target-dir",
    };
    try {
      validateImportOptions(extraArgs);
      fail("Expected InvalidOptionsException");
    } catch(SqoopOptions.InvalidOptionsException ioe) {
      // Expected
    }
  }

  //test incompatability of --delete-target-dir with incremental import
  public void testDeleteWithIncrementalImport() throws Exception {
    String [] extraArgs = {
      "--incremental", "append",
      "--delete-target-dir",
    };
    try {
      validateImportOptions(extraArgs);
      fail("Expected InvalidOptionsException");
    } catch(SqoopOptions.InvalidOptionsException ioe) {
      // Expected
    }
  }

  // test that hbase bulk load import with table name and target dir
  // passes validation
  public void testHBaseBulkLoad() throws Exception {
    String [] extraArgs = {
        longArgument(BaseSqoopTool.HBASE_BULK_LOAD_ENABLED_ARG),
        longArgument(BaseSqoopTool.TARGET_DIR_ARG), "./test",
        longArgument(BaseSqoopTool.HBASE_TABLE_ARG), "test_table",
        longArgument(BaseSqoopTool.HBASE_COL_FAM_ARG), "d"};

    validateImportOptions(extraArgs);
  }

  // test that hbase bulk load import with a missing --hbase-table fails
  public void testHBaseBulkLoadMissingHbaseTable() throws Exception {
    String [] extraArgs = {
        longArgument(BaseSqoopTool.HBASE_BULK_LOAD_ENABLED_ARG),
        longArgument(BaseSqoopTool.TARGET_DIR_ARG), "./test"};
    try {
      validateImportOptions(extraArgs);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // Expected
    }
  }

  private static String longArgument(String argument) {
    return String.format("--%s", argument);
  }

  public void testRelaxedIsolation() throws Exception {
    String extraArgs[] = {
      "--relaxed-isolation",
    };
    validateImportOptions(extraArgs);
  }

  public void testResetToOneMapper() throws Exception {
    String extraArgs[] = {
      "--autoreset-to-one-mapper",
    };
    validateImportOptions(extraArgs);
  }

  public void testResetToOneMapperAndSplitBy() throws Exception {
    String extraArgs[] = {
      "--autoreset-to-one-mapper",
      "--split-by",
      "col0",
    };
    try {
      validateImportOptions(extraArgs);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // Expected
    }
  }
}
