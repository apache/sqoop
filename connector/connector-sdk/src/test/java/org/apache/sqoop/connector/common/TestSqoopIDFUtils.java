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
package org.apache.sqoop.connector.common;

import static org.testng.Assert.*;
import static org.apache.sqoop.connector.common.SqoopIDFUtils.*;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.type.AbstractComplexListType;
import org.apache.sqoop.schema.type.Array;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Text;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSqoopIDFUtils {

  public static String getByteFieldString(byte[] byteFieldData) {
    try {
      return new StringBuilder("'").append(new String(byteFieldData, BYTE_FIELD_CHARSET))
          .append("'").toString();
    } catch (UnsupportedEncodingException e) {
      // Should never get to this point because ISO-8859-1 is a standard codec.
      return null;
    }
  }

  @Test
  public void testEncloseStringWithQuotes() {
    String test = "test";
    String quotedText = encloseWithQuotes(test);
    assertEquals(quotedText, "'test'");

  }

  @Test
  public void testStringWithQuotesToEncloseStringWithQuotes() {
    String test = "'test'";
    String quotedText = encloseWithQuotes(test);
    assertEquals(quotedText, "''test''");

  }

  @Test
  public void testRemoveQuotes() {
    String test = "'test'";
    String quotedText = removeQuotes(test);
    assertEquals(quotedText, "test");
  }

  @Test
  public void testStringWithNoQuotesRemoveQuotes() {
    String test = "test";
    String quotedText = removeQuotes(test);
    assertEquals(quotedText, "test");
  }

  @Test
  public void testStingWithNoQuotesRemoveQuotes() {
    String test = "test";
    String quotedText = removeQuotes(test);
    assertEquals(quotedText, "test");
  }

  @Test
  public void testExample1EncodeToCSVString() {
    String test = "test";
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'test'");
  }

  @Test
  public void testExample2EncodeToCSVString() {
    String test = "test,test1";
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'test,test1'");
  }

  @Test
  public void testExample3EncodeToCSVString() {
    String test = "test,'test1";
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'test,\\'test1'");
  }

  @Test
  public void testExample4EncodeToCSVString() {
    String test = "test,\"test1";
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'test,\\\"test1'");
  }

  @Test
  public void testExample4ToString() {
    String test = "'test,\\\"test1'";
    String expectedString = "test,\"test1";
    String toString = toText(test);
    assertEquals(toString, expectedString);
  }

  @Test
  public void testExample5EncodeToCSVString() {
    String test = new String(new char[] { 0x0A });
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'\\n'");
  }

  @Test
  public void testExample5ToString() {
    String test = "'\\n'";
    String expectedString = new String(new char[] { 0x0A });
    String toString = toText(test);
    assertEquals(toString, expectedString);
  }

  @Test
  public void testExample6EncodeToCSVString() {
    String test = new String(new char[] { 0x0D });
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'\\r'");
  }

  @Test
  public void testToCSVFixedPointWithIntSignedAsInteger() {
    Column col = new FixedPoint("ft", 2L, true);
    String encodedText = toCSVFixedPoint(1, col);
    assertTrue(Integer.valueOf(encodedText) instanceof Integer);
  }

  @Test
  public void testToCSVFixedPointWithIntSize4SignedAsInteger() {
    Column col = new FixedPoint("ft", 4L, true);
    String encodedText = toCSVFixedPoint(1, col);
    assertTrue(Integer.valueOf(encodedText) instanceof Integer);
  }

  @Test
  public void testToCSVFixedPointWithLongSignedAsInteger() {
    Column col = new FixedPoint("ft", 4L, true);
    Long test = 459999999444L;
    String encodedText = toCSVFixedPoint(test, col);
    assertTrue(Integer.valueOf(encodedText) instanceof Integer);
  }

  @Test
  public void testToCSVFixedPointWithIntSize2UnSignedAsInteger() {
    Column col = new FixedPoint("ft", 2L, false);
    Integer test = 45999999;
    String encodedText = toCSVFixedPoint(test, col);
    assertTrue(Integer.valueOf(encodedText) instanceof Integer);
  }

  @Test
  public void testToCSVFixedPointWithIntSize16UnSignedAsLong() {
    Column col = new FixedPoint("ft", 16L, false);
    Long test = 1000000000L;
    String encodedText = toCSVFixedPoint(test, col);
    assertTrue(Long.valueOf(encodedText) instanceof Long);
  }

  @Test
  public void testToCSVFixedPointWithIntUnSignedAsLong() {
    Column col = new FixedPoint("ft", 4L, false);
    // java does not have a concept of unsigned int, so it has to be a long for
    // testing
    long test = 100000000900000L;
    String encodedText = toCSVFixedPoint(test, col);
    assertTrue(Long.valueOf(encodedText) instanceof Long);
  }

  @Test
  public void testToCSVFixedPointWithLongAsInt() {
    Column col = new FixedPoint("ft", 2L, false);
    // java does not have a concept of unsigned int, so it has to be a long for
    // testing
    String encodedText = toCSVFixedPoint(new Long(Integer.MAX_VALUE), col);
    assertEquals("2147483647", encodedText);
  }

  @Test
  public void testToCSVFixedPointWithIntAsLong() {
    Column col = new FixedPoint("ft", 4L, false);
    // java does not have a concept of unsigned int, so it has to be a long for
    // testing
    String encodedText = toCSVFixedPoint(Integer.MAX_VALUE, col);
    assertEquals("2147483647", encodedText);
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testToCSVFixedPointWithBadNumberAsLong() {
    Column col = new FixedPoint("ft", 4L, false);
    toCSVFixedPoint("lame", col);
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testToCSVFixedPointWithBadNumberAsInteger() {
    Column col = new FixedPoint("ft", 2L, false);
    toCSVFixedPoint("lame", col);
  }

  @Test
  public void testToFixedPointReturnsInt() {
    Column col = new FixedPoint("fixt", 4L, true);
    assertTrue(toFixedPoint("233", col) instanceof Integer);
  }

  @Test
  public void testToFixedPointReturnsLong() {
    Column col = new FixedPoint("fixt", 8L, true);
    assertTrue(toFixedPoint("233", col) instanceof Long);
  }

  @Test
  public void testToFixedPointUnsignedReturnsLong() {
    Column col = new FixedPoint("fixt", 4L, false);
    assertTrue(toFixedPoint("2333333333333333", col) instanceof Long);
  }

  @Test
  public void testToCSVFloatingPointAsFloat() {
    Column col = new FloatingPoint("ft", 2L);
    Float test = 2.3F;
    String encodedText = toCSVFloatingPoint(test, col);
    assertTrue(Float.valueOf(encodedText) instanceof Float);
  }

  @Test
  public void testToFloatingPointReturnsFloat() {
    Column col = new FloatingPoint("ft", 4L);
    assertTrue(toFloatingPoint("2.33", col) instanceof Float);
  }

  @Test
  public void testToCSVFloatingPointAsDouble() {
    Column col = new FloatingPoint("ft", 5L);
    Double test = 2.3D;
    String encodedText = toCSVFloatingPoint(test, col);
    assertTrue(Double.valueOf(encodedText) instanceof Double);
  }

  @Test
  public void testToFloatingPointReturnsDouble() {
    Column col = new FloatingPoint("ft", 8L);
    assertTrue(toFloatingPoint("2.33", col) instanceof Double);
  }

  @Test
  public void testEncodeByteToCSVString() {
    // byte[0] = \r byte[1] = -112, byte[1] = 54 - 2's complements
    byte[] bytes = new byte[] { (byte) 0x0D, (byte) -112, (byte) 54 };
    String encodedText = toCSVByteArray(bytes);
    String expectedText = getByteFieldString(bytes).replaceAll("\r", "\\\\r");
    assertEquals(encodedText, expectedText);
  }

  @Test
  public void testEncodeArrayIntegersToCSVString() {
    List<Integer> list = new ArrayList<Integer>();
    list.add(1);
    list.add(2);
    AbstractComplexListType array = new Array("a", new Text("t"));
    String encodedText = toCSVList(list.toArray(), array);
    assertEquals(encodedText, "'[1,2]'");
  }

  @Test
  public void testEncodeArrayStringsToCSVString() {
    List<String> list = new ArrayList<String>();
    list.add("A");
    list.add("B");
    AbstractComplexListType array = new Array("a", new Text("t"));
    String encodedText = toCSVList(list.toArray(), array);
    assertEquals(encodedText, "'[\"A\",\"B\"]'");
  }

  @Test
  public void testEncodeMapToCSVString() {
    List<String> list = new ArrayList<String>();
    list.add("A");
    list.add("B");
    Map<Object, Object> map = new HashMap<Object, Object>();
    map.put("A", list);
    org.apache.sqoop.schema.type.Map mapCol = new org.apache.sqoop.schema.type.Map("a", new Text("t"), new Array("r", new Text(
        "tr")));
    String encodedText = toCSVMap(map, mapCol);
    assertEquals(encodedText, "'{\"A\":[\"A\",\"B\"]}'");
  }

  @Test
  public void testParseCSVString() {

    String csv = "'hello, world','34',45";
    String[] arr = parseCSVString(csv);
    assertEquals(arr.length, 3);
    assertEquals(arr[0], "'hello, world'");
    assertEquals(arr[1], "'34'");
    assertEquals(arr[2], "45");

  }

  @Test
  public void testToDecimaPointReturnsDecimal() {
    String text = "23.44444444";
    Decimal col = new Decimal("dd", 4, 2);
    assertTrue(toDecimal(text, col) instanceof BigDecimal);
    BigDecimal bd = (BigDecimal) toDecimal(text, col);
    assertEquals("23.44", toCSVDecimal(bd));
  }

  @Test
  public void testToDecimaPoint2ReturnsDecimal() {
    String text = "23.44444444";
    Decimal col = new Decimal("dd", 8, 2);
    assertTrue(toDecimal(text, col) instanceof BigDecimal);
    BigDecimal bd = (BigDecimal) toDecimal(text, col);
    assertEquals("23.444444", toCSVDecimal(bd));
  }

  @Test
  public void testToDecimaPointNoScaleNoPrecisionReturnsDecimal() {
    String text = "23.44444444";
    Decimal col = new Decimal("dd", null, null);
    assertTrue(toDecimal(text, col) instanceof BigDecimal);
    BigDecimal bd = (BigDecimal) toDecimal(text, col);
    assertEquals("23.44444444", toCSVDecimal(bd));
  }

  @Test
  public void testEscaping() {
    String[][] testData = new String[][]{
        {"\0", "'\\0'"},
        {"\\0", "'\\\\0'"},
        {"\\\0", "'\\\\\\0'"},
        {"\\\\0", "'\\\\\\\\0'"},
        {"\n", "'\\n'"},
        {"\\n", "'\\\\n'"},
        {"\\\n", "'\\\\\\n'"},
        {"\\\\n", "'\\\\\\\\n'"},
        {"\r", "'\\r'"},
        {"\\r", "'\\\\r'"},
        {"\\\r", "'\\\\\\r'"},
        {"\\\\r", "'\\\\\\\\r'"},
        {Character.toString((char)0x1A), "'\\Z'"},
        {"\\Z", "'\\\\Z'"},
        {"\\" + Character.toString((char)0x1A), "'\\\\\\Z'"},
        {"\\\\Z", "'\\\\\\\\Z'"},
        {"\"", "'\\\"'"},
        {"\\\"", "'\\\\\\\"'"},
        {"\\\\\"", "'\\\\\\\\\\\"'"},
        {"\\\\\\\"", "'\\\\\\\\\\\\\\\"'"},
        {"'", "'\\''"},
        {"\\'", "'\\\\\\''"},
        {"\\\\'", "'\\\\\\\\\\''"},
        {"\\\\\\'", "'\\\\\\\\\\\\\\''"}
    };

    for (String[] testDatum : testData) {
      String csvData = SqoopIDFUtils.toCSVString(testDatum[0]);

      assertEquals(csvData, testDatum[1]);

      assertEquals(SqoopIDFUtils.toText(csvData), testDatum[0]);
    }
  }
}
