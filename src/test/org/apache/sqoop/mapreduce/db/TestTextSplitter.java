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
package org.apache.sqoop.mapreduce.db;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.sqoop.validation.ValidationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Rule;

import org.junit.rules.ExpectedException;


/**
 * Test that the TextSplitter implementation creates a sane set of splits.
 */
public class TestTextSplitter {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public String formatArray(Object [] ar) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (Object val : ar) {
      if (!first) {
        sb.append(", ");
      }

      sb.append(val.toString());
      first = false;
    }

    sb.append("]");
    return sb.toString();
  }

  public void assertArrayEquals(Object [] expected, Object [] actual) {
    for (int i = 0; i < expected.length; i++) {
      try {
        assertEquals("Failure at position " + i + "; got " + actual[i]
            + " instead of " + expected[i]
            + "; actual array is " + formatArray(actual),
            expected[i], actual[i]);
      } catch (ArrayIndexOutOfBoundsException oob) {
        fail("Expected array with " + expected.length
            + " elements; got " + actual.length
            + ". Actual array is " + formatArray(actual));
      }
    }

    if (actual.length > expected.length) {
      fail("Actual array has " + actual.length
          + " elements; expected " + expected.length
          + ". Actual array is " + formatArray(actual));
    }
  }

  @Test
  public void testStringConvertEmpty() {
    TextSplitter splitter = new TextSplitter();
    BigDecimal emptyBigDec = splitter.stringToBigDecimal("");
    assertEquals(BigDecimal.ZERO, emptyBigDec);
  }

  @Test
  public void testBigDecConvertEmpty() {
    TextSplitter splitter = new TextSplitter();
    String emptyStr = splitter.bigDecimalToString(BigDecimal.ZERO);
    assertEquals("", emptyStr);
  }

  @Test
  public void testConvertA() {
    TextSplitter splitter = new TextSplitter();
    String out = splitter.bigDecimalToString(splitter.stringToBigDecimal("A"));
    assertEquals("A", out);
  }

  @Test
  public void testConvertZ() {
    TextSplitter splitter = new TextSplitter();
    String out = splitter.bigDecimalToString(splitter.stringToBigDecimal("Z"));
    assertEquals("Z", out);
  }

  @Test
  public void testConvertThreeChars() {
    TextSplitter splitter = new TextSplitter();
    String out = splitter.bigDecimalToString(
        splitter.stringToBigDecimal("abc"));
    assertEquals("abc", out);
  }

  @Test
  public void testConvertStr() {
    TextSplitter splitter = new TextSplitter();
    String out = splitter.bigDecimalToString(
        splitter.stringToBigDecimal("big str"));
    assertEquals("big str", out);
  }

  @Test
  public void testConvertChomped() {
    TextSplitter splitter = new TextSplitter();
    String out = splitter.bigDecimalToString(
        splitter.stringToBigDecimal("AVeryLongStringIndeed"));
    assertEquals("AVeryLon", out);
  }

  @Test
  public void testAlphabetSplit() throws SQLException, ValidationException {
    // This should give us 25 splits, one per letter.
    TextSplitter splitter = new TextSplitter();
    List<String> splits = splitter.split(25, "A", "Z", "");
    String [] expected = { "A", "B", "C", "D", "E", "F", "G", "H", "I",
        "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U",
        "V", "W", "X", "Y", "Z", };
    assertArrayEquals(expected, splits.toArray(new String [0]));
  }

  @Test
  public void testAlphabetSplitWhenMinStringGreaterThanMaxString() throws SQLException, ValidationException {
    TextSplitter splitter = new TextSplitter();

    thrown.expect(ValidationException.class);
    thrown.reportMissingExceptionWithMessage("Expected ValidationException during splitting " +
        "when min string greater than max string");
    splitter.split(4, "Z", "A", "");
  }

  @Test
  public void testCommonPrefix() throws SQLException, ValidationException {
    // Splits between 'Hand' and 'Hardy'
    TextSplitter splitter = new TextSplitter();
    List<String> splits = splitter.split(5, "nd", "rdy", "Ha");
    // Don't check for exact values in the middle, because the splitter
    // generates some ugly Unicode-isms. But do check that we get multiple
    // splits and that it starts and ends on the correct points.
    assertEquals("Hand", splits.get(0));
    assertEquals("Hardy", splits.get(splits.size() -1));
    assertEquals(6, splits.size());
  }

  @Test
  public void testNChar() throws SQLException {
    // Splits between 'Hand' and 'Hardy'
    NTextSplitter splitter = new NTextSplitter();
    assertEquals(true, splitter.isUseNCharStrings());
    TextSplitter splitter2 = new TextSplitter();
    assertEquals(false, splitter2.isUseNCharStrings());
  }

}
