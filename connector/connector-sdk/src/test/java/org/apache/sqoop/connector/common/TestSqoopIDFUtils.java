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

import org.apache.sqoop.schema.type.AbstractComplexListType;
import org.apache.sqoop.schema.type.Array;
import org.apache.sqoop.schema.type.Text;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSqoopIDFUtils {

  public static String getByteFieldString(byte[] byteFieldData) {
    try {
      return new StringBuilder("'").append(new String(byteFieldData, BYTE_FIELD_CHARSET)).append("'").toString();
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

  public void testExample5EncodeToCSVString() {
    String test = new String(new char[] { 0x0A });
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'\\n'");
  }

  public void testExample5ToString() {
    String test = "'\\n'";
    String expectedString = new String(new char[] { 0x0A });
    String toString = toText(test);
    assertEquals(toString, expectedString);
  }

  public void testExample6EncodeToCSVString() {
    String test = new String(new char[] { 0x0D });
    String encodedText = toCSVString(test);
    assertEquals(encodedText, "'\\r'");
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

    String csv= "'hello, world','34',45";
    String[] arr = parseCSVString(csv);
    assertEquals(arr.length, 3);
    assertEquals(arr[0], "'hello, world'");
    assertEquals(arr[1], "'34'");
    assertEquals(arr[2], "45");
    
  }


}