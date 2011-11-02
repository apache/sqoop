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

package com.cloudera.sqoop.lib;

import junit.framework.TestCase;


/**
 * Test that the field formatter works in a variety of configurations.
 */
public class TestFieldFormatter extends TestCase {

  public void testAllEmpty() {
    String result = FieldFormatter.escapeAndEnclose("",
        new DelimiterSet(DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR,
        DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR, false));
    assertEquals("", result);
  }

  public void testNullArgs() {
    assertNull(FieldFormatter.escapeAndEnclose(null,
      new DelimiterSet('\"', DelimiterSet.NULL_CHAR, '\"', '\\', false)));
  }

  public void testBasicStr() {
    String result = FieldFormatter.escapeAndEnclose("foo",
        DelimiterSet.DEFAULT_DELIMITERS);
    assertEquals("foo", result);
  }

  public void testEscapeSlash() {
    String result = FieldFormatter.escapeAndEnclose("foo\\bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("foo\\\\bar", result);
  }

  public void testMustEnclose() {
    String result = FieldFormatter.escapeAndEnclose("foo",
        new DelimiterSet(',', '\n', '\"', DelimiterSet.NULL_CHAR, true));
    assertEquals("\"foo\"", result);
  }

  public void testEncloseComma1() {
    String result = FieldFormatter.escapeAndEnclose("foo,bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("\"foo,bar\"", result);
  }

  public void testEncloseComma2() {
    String result = FieldFormatter.escapeAndEnclose("foo,bar",
        new DelimiterSet(',', ',', '\"', '\\', false));
    assertEquals("\"foo,bar\"", result);
  }

  public void testNoNeedToEnclose() {
    String result = FieldFormatter.escapeAndEnclose(
        "just another string",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("just another string", result);
  }

  public void testCannotEnclose() {
    // Can't enclose because encloser is nul.
    // This should escape the comma instead.
    String result = FieldFormatter.escapeAndEnclose("foo,bar",
        new DelimiterSet(',', '\n', DelimiterSet.NULL_CHAR, '\\', false));

    assertEquals("foo\\,bar", result);
  }

  public void testEmptyCharToEscapeString() {
    // test what happens when the escape char is null. It should encode the
    // null char.

    char nul = DelimiterSet.NULL_CHAR;
    String s = "" + nul;
    assertEquals("\000", s);
  }

  public void testEscapeCentralQuote() {
    String result = FieldFormatter.escapeAndEnclose("foo\"bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("foo\\\"bar", result);
  }

  public void testEscapeMultiCentralQuote() {
    String result = FieldFormatter.escapeAndEnclose("foo\"\"bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("foo\\\"\\\"bar", result);
  }

  public void testDoubleEscape() {
    String result = FieldFormatter.escapeAndEnclose("foo\\\"bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("foo\\\\\\\"bar", result);
  }

  public void testReverseEscape() {
    String result = FieldFormatter.escapeAndEnclose("foo\"\\bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("foo\\\"\\\\bar", result);
  }

  public void testQuotedEncloser() {
    String result = FieldFormatter.escapeAndEnclose("foo\",bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("\"foo\\\",bar\"", result);
  }

  public void testQuotedEscape() {
    String result = FieldFormatter.escapeAndEnclose("foo\\,bar",
        new DelimiterSet(',', '\n', '\"', '\\', false));
    assertEquals("\"foo\\\\,bar\"", result);
  }
}
