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
import java.util.ArrayList;
import java.util.List;

import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import com.cloudera.sqoop.mapreduce.db.TextSplitter;
import org.apache.hadoop.mapreduce.InputSplit;
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

  @Test
  public void testSplit() throws Exception {
    System.out.println("Generating splits for a textual index column.");
    System.out.println("If your database sorts in a case-insensitive order, "
            + "this may result in a partial import or duplicate records.");
    System.out.println("You are strongly encouraged to choose an integral split column.");

    org.apache.sqoop.mapreduce.db.TextSplitter textSplitter = new org.apache.sqoop.mapreduce.db.TextSplitter();
    boolean useNCharStrings = false;
    String colName = "produce_name";
    String minString = "1231";
    String maxString = "12324";

    boolean minIsNull = false;

    // If the min value is null, switch it to an empty string instead for
    // purposes of interpolation. Then add [null, null] as a special case
    // split.
    if (null == minString) {
      minString = "";
      minIsNull = true;
    }

    if (null == maxString) {
      // If the max string is null, then the min string has to be null too.
      // Just return a special split for this case.
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              colName + " IS NULL", colName + " IS NULL"));
      return;
    }

    // Use this as a hint. May need an extra task if the size doesn't
    // divide cleanly.
    // 本地是1
    // 远程默认是2
    int numSplits = 3;

    String lowClausePrefix = colName + " >= " + (useNCharStrings ? "N'" : "'");
    String highClausePrefix = colName + " < " + (useNCharStrings ? "N'" : "'");

    // If there is a common prefix between minString and maxString, establish
    // it and pull it out of minString and maxString.
    int maxPrefixLen = Math.min(minString.length(), maxString.length());
    int sharedLen;
    for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
      char c1 = minString.charAt(sharedLen);
      char c2 = maxString.charAt(sharedLen);
      if (c1 != c2) {
        break;
      }
    }

    // The common prefix has length 'sharedLen'. Extract it from both.
    String commonPrefix = minString.substring(0, sharedLen);
    minString = minString.substring(sharedLen);
    maxString = maxString.substring(sharedLen);

    List<String> splitStrings = textSplitter.split(numSplits, minString, maxString,
            commonPrefix);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // Convert the list of split point strings into an actual set of
    // InputSplits.
    String start = splitStrings.get(0);
    for (int i = 1; i < splitStrings.size(); i++) {
      String end = splitStrings.get(i);

      if (i == splitStrings.size() - 1) {
        // This is the last one; use a closed interval.
        splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                lowClausePrefix + start + "'", colName
                + " <= " + (useNCharStrings ? "N'" : "'") + end + "'"));
      } else {
        // Normal open-interval case.
        splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                lowClausePrefix + start + "'", highClausePrefix + end + "'"));
      }

      start = end;
    }

    if (minIsNull) {
      // Add the special null split at the end.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              colName + " IS NULL", colName + " IS NULL"));
    }
    System.out.println(splits);
  }
}
