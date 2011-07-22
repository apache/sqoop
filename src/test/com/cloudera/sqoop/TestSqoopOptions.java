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

package com.cloudera.sqoop;

import java.util.Properties;

import junit.framework.TestCase;

import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.tool.ImportTool;

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

  public void testPropertySerialization() {
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

    Properties outProps = out.writeProperties();

    SqoopOptions in = new SqoopOptions();
    in.loadProperties(outProps);

    Properties inProps = in.writeProperties();

    assertEquals("properties don't match", outProps, inProps);
  }
}
