/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.connector.idf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Array;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.Text;
import org.junit.Before;
import org.junit.Test;

public class TestCSVIntermediateDataFormat {

  private IntermediateDataFormat<?> dataFormat;

  @Before
  public void setUp() {
    dataFormat = new CSVIntermediateDataFormat();
  }

  private String getByteFieldString(byte[] byteFieldData) {
    try {
      return new StringBuilder("'")
          .append(new String(byteFieldData, CSVIntermediateDataFormat.BYTE_FIELD_CHARSET))
          .append("'").toString();
    } catch (UnsupportedEncodingException e) {
      // Should never get to this point because ISO-8859-1 is a standard codec.
      return null;
    }
  }

  //**************test cases for null and empty input*******************

  @Test
  public void testNullInputAsCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));
    dataFormat.setSchema(schema);
    dataFormat.setTextData(null);
    Object[] out = dataFormat.getObjectData();
    assertNull(out);
  }

  @Test(expected=SqoopException.class)
  public void testEmptyInputAsCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));
    dataFormat.setSchema(schema);
    dataFormat.setTextData("");
    dataFormat.getObjectData();
  }

  //**************test cases for primitive types( text, number, bytearray)*******************

  @Test
  public void testInputAsCSVTextInCSVTextOut() {
    String testData = "'ENUM',10,34,'54','random data'," + getByteFieldString(new byte[] { (byte) -112, (byte) 54})
      + ",'" + String.valueOf(0x0A) + "'";
    dataFormat.setTextData(testData);
    assertEquals(testData, dataFormat.getTextData());
  }

  @Test
  public void testInputAsCSVTextInObjectOut() {

    //byte[0] = -112, byte[1] = 54 - 2's complements
    String testData = "10,34,'54','random data'," + getByteFieldString(new byte[] { (byte) -112, (byte) 54})
      + ",'\\n','TEST_ENUM'";
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"))
        .addColumn(new org.apache.sqoop.schema.type.Enum("7"));

    dataFormat.setSchema(schema);
    dataFormat.setTextData(testData);

    Object[] out = dataFormat.getObjectData();

    assertEquals(new Long(10),out[0]);
    assertEquals(new Long(34),out[1]);
    assertEquals("54",out[2]);
    assertEquals("random data",out[3]);
    assertEquals(-112, ((byte[]) out[4])[0]);
    assertEquals(54, ((byte[])out[4])[1]);
    assertEquals("\n", out[5].toString());
    assertEquals("TEST_ENUM", out[6].toString());
  }

  @Test
  public void testInputAsObjectArayInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"))
        .addColumn(new org.apache.sqoop.schema.type.Enum("7"));
    dataFormat.setSchema(schema);

    byte[] byteFieldData = new byte[] { (byte) 0x0D, (byte) -112, (byte) 54};
    Object[] in = new Object[7];
    in[0] = new Long(10);
    in[1] = new Long(34);
    in[2] = "54";
    in[3] = "random data";
    in[4] = byteFieldData;
    in[5] = new String(new char[] { 0x0A });
    in[6] = "TEST_ENUM";

    dataFormat.setObjectData(in);

    //byte[0] = \r byte[1] = -112, byte[1] = 54 - 2's complements
    String testData = "10,34,'54','random data'," +
        getByteFieldString(byteFieldData).replaceAll("\r", "\\\\r") + ",'\\n','TEST_ENUM'";
    assertEquals(testData, dataFormat.getTextData());
  }

  @Test
  public void testObjectArrayInObjectArrayOut() {
    //Test escapable sequences too.
    //byte[0] = -112, byte[1] = 54 - 2's complements
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"))
        .addColumn(new org.apache.sqoop.schema.type.Enum("7"));

    dataFormat.setSchema(schema);

    Object[] in = new Object[7];
    in[0] = new Long(10);
    in[1] = new Long(34);
    in[2] = "54";
    in[3] = "random data";
    in[4] = new byte[] { (byte) -112, (byte) 54};
    in[5] = new String(new char[] { 0x0A });
    in[6] = "TEST_ENUM";
    Object[] inCopy = new Object[7];
    System.arraycopy(in,0,inCopy,0,in.length);

    // Modifies the input array, so we use the copy to confirm
    dataFormat.setObjectData(in);

    assertTrue(Arrays.deepEquals(inCopy, dataFormat.getObjectData()));
  }

  @Test
  public void testObjectArrayWithNullInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"))
        .addColumn(new org.apache.sqoop.schema.type.Enum("7"));

    dataFormat.setSchema(schema);

    byte[] byteFieldData = new byte[] { (byte) 0x0D, (byte) -112, (byte) 54};
    Object[] in = new Object[7];
    in[0] = new Long(10);
    in[1] = new Long(34);
    in[2] = null;
    in[3] = "random data";
    in[4] = byteFieldData;
    in[5] = new String(new char[] { 0x0A });
    in[6] = "TEST_ENUM";

    dataFormat.setObjectData(in);

    //byte[0] = \r byte[1] = -112, byte[1] = 54 - 2's complements
    String testData = "10,34,NULL,'random data'," +
        getByteFieldString(byteFieldData).replaceAll("\r", "\\\\r") + ",'\\n','TEST_ENUM'";
    assertEquals(testData, dataFormat.getTextData());
  }

  @Test
  public void testStringFullRangeOfCharacters() {
    Schema schema = new Schema("test");
    schema.addColumn(new Text("1"));

    dataFormat.setSchema(schema);

    char[] allCharArr = new char[256];
    for(int i = 0; i < allCharArr.length; ++i) {
      allCharArr[i] = (char)i;
    }
    String strData = new String(allCharArr);

    Object[] in = {strData};
    Object[] inCopy = new Object[1];
    System.arraycopy(in, 0, inCopy, 0, in.length);

    // Modifies the input array, so we use the copy to confirm
    dataFormat.setObjectData(in);

    assertEquals(strData, dataFormat.getObjectData()[0]);
    assertTrue(Arrays.deepEquals(inCopy, dataFormat.getObjectData()));
  }

  @Test
  public void testByteArrayFullRangeOfCharacters() {
    Schema schema = new Schema("test");
    schema.addColumn(new Binary("1"));
    dataFormat.setSchema(schema);

    byte[] allCharByteArr = new byte[256];
    for (int i = 0; i < allCharByteArr.length; ++i) {
      allCharByteArr[i] = (byte) i;
    }

    Object[] in = {allCharByteArr};
    Object[] inCopy = new Object[1];
    System.arraycopy(in, 0, inCopy, 0, in.length);

    // Modifies the input array, so we use the copy to confirm
    dataFormat.setObjectData(in);
    assertTrue(Arrays.deepEquals(inCopy, dataFormat.getObjectData()));
  }

  // **************test cases for date*******************

  @Test
  public void testDateWithCSVTextInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new Date("1"));
    dataFormat.setSchema(schema);
    dataFormat.setTextData("'2014-10-01'");
    assertEquals("'2014-10-01'", dataFormat.getTextData());
  }

  @Test
  public void testDateWithCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new Date("1"));
    dataFormat.setSchema(schema);
    dataFormat.setTextData("'2014-10-01'");
    org.joda.time.LocalDate date = new org.joda.time.LocalDate(2014, 10, 01);
    assertEquals(date.toString(), dataFormat.getObjectData()[0].toString());
  }

  @Test
  public void testDateWithObjectArrayInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new Date("1")).addColumn(new Text("2"));
    dataFormat.setSchema(schema);
    org.joda.time.LocalDate date = new org.joda.time.LocalDate(2014, 10, 01);
    Object[] in = { date, "test" };
    dataFormat.setObjectData(in);
    assertEquals("'2014-10-01','test'", dataFormat.getTextData());
  }

  @Test
  public void testDateWithObjectArrayInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new Date("1"));
    dataFormat.setSchema(schema);
    org.joda.time.LocalDate date = new org.joda.time.LocalDate(2014, 10, 01);
    Object[] in = { date };
    dataFormat.setObjectData(in);
    assertEquals(date.toString(), dataFormat.getObjectData()[0].toString());
  }

  // **************test cases for dateTime*******************

  @Test
  public void testDateTimeWithCSVTextInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    dataFormat.setSchema(schema);

    dataFormat.setTextData("'2014-10-01 12:00:00'");
    assertEquals("'2014-10-01 12:00:00'", dataFormat.getTextData());
  }

  @Test
  public void testDateTimeWithCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    dataFormat.setSchema(schema);

    dataFormat.setTextData("'2014-10-01 12:00:00'");
    assertEquals("2014-10-01T12:00:00.000-07:00", dataFormat.getObjectData()[0].toString());
  }

  @Test
  public void testDateTimeWithObjectInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    dataFormat.setSchema(schema);
    org.joda.time.DateTime dateTime = new org.joda.time.DateTime(2014, 10, 01, 12, 0, 0, 0);
    Object[] in = { dateTime };
    dataFormat.setObjectData(in);
    assertEquals("'2014-10-01 12:00:00.000Z'", dataFormat.getTextData());
  }

  @Test
  public void testLocalDateTimeWithObjectInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    dataFormat.setSchema(schema);
    org.joda.time.LocalDateTime dateTime = new org.joda.time.LocalDateTime(2014, 10, 01, 12, 0, 0,
        0);
    Object[] in = { dateTime };
    dataFormat.setObjectData(in);
    assertEquals("'2014-10-01 12:00:00.000Z'", dataFormat.getTextData());
  }

  @Test
  public void testDateTimePrecisionWithCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    dataFormat.setSchema(schema);

    for (String dateTime : new String[] { "'2014-10-01 12:00:00.000'" }) {
      dataFormat.setTextData(dateTime);
      assertEquals("2014-10-01T12:00:00.000-07:00", dataFormat.getObjectData()[0].toString());
    }
  }

  /**
   * In ISO8601 "T" is used as date-time separator. Unfortunately in the real
   * world, database (confirmed with mysql and postgres) might return a datetime
   * string with a space as separator. The test case intends to check, whether
   * such datetime string can be handled expectedly.
   */
  @Test
  public void testDateTimeISO8601Alternative() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    dataFormat.setSchema(schema);

    for (String dateTime : new String[] { "'2014-10-01 12:00:00'", "'2014-10-01 12:00:00.000'" }) {
      dataFormat.setTextData(dateTime);
      assertEquals("2014-10-01T12:00:00.000-07:00", dataFormat.getObjectData()[0].toString());
    }
  }

  @Test
  public void testBit() {
    Schema schema = new Schema("test");
    schema.addColumn(new Bit("1"));
    dataFormat.setSchema(schema);

    for (String trueBit : new String[]{
        "true", "TRUE", "1"
    }) {
      dataFormat.setTextData(trueBit);
      assertTrue((Boolean) dataFormat.getObjectData()[0]);
    }

    for (String falseBit : new String[]{
        "false", "FALSE", "0"
    }) {
      dataFormat.setTextData(falseBit);
      assertFalse((Boolean) dataFormat.getObjectData()[0]);
    }
  }

  //**************test cases for arrays*******************
  @Test
  public void testArrayOfStringWithObjectArrayInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1", new Text("text")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArray = { "A", "B" };
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenArray;
    data[1] = "text";
    dataFormat.setObjectData(data);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(Arrays.toString(givenArray), Arrays.toString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testArrayOfStringWithCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1", new Text("text")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArray = { "A", "B" };
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenArray;
    data[1] = "text";
    String testData = "'[\"A\",\"B\"]','text'";
    dataFormat.setTextData(testData);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(Arrays.toString(givenArray), Arrays.toString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testArrayOfStringWithObjectArrayInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1", new Text("text")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArray = { "A", "B" };
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenArray;
    data[1] = "text";
    String testData = "'[\"A\",\"B\"]','text'";
    dataFormat.setObjectData(data);
    assertEquals(testData, dataFormat.getTextData());
  }

  @Test
  public void testArrayOfStringWithCSVTextInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1", new Text("text")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    String testData = "'[\"A\",\"B\"]','text'";
    dataFormat.setTextData(testData);
    assertEquals(testData, dataFormat.getTextData());
  }

  @Test
  public void testArrayOfComplexStrings() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1", new Text("text")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArray = { "A''\"ssss", "Bss###''" };
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenArray;
    data[1] = "tex''t";
    dataFormat.setObjectData(data);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(Arrays.toString(givenArray), Arrays.toString(expectedArray));
    assertEquals("tex''t", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testArrayOfIntegers() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1", new FixedPoint("fn")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArray = { 1, 2 };
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenArray;
    data[1] = "text";
    dataFormat.setObjectData(data);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(Arrays.toString(givenArray), Arrays.toString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testListOfIntegers() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1", new FixedPoint("fn")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    List<Integer> givenList = new ArrayList<Integer>();
    givenList.add(1);
    givenList.add(1);
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenList.toArray();
    data[1] = "text";
    dataFormat.setObjectData(data);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(givenList.toString(), Arrays.toString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  public void testSetOfIntegers() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Set("1", new FixedPoint("fn")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Set<Integer> givenSet = new HashSet<Integer>();
    givenSet.add(1);
    givenSet.add(3);
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenSet.toArray();
    data[1] = "text";
    dataFormat.setObjectData(data);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(givenSet.toString(), Arrays.toString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testArrayOfDecimals() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1",
        new org.apache.sqoop.schema.type.Decimal("deci")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArray = { 1.22, 2.444 };
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenArray;
    data[1] = "text";
    dataFormat.setObjectData(data);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(Arrays.toString(givenArray), Arrays.toString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testArrayOfObjectsWithObjectArrayInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1",
        new org.apache.sqoop.schema.type.Array("array", new FixedPoint("ft"))));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArrayOne = { 11, 12 };
    Object[] givenArrayTwo = { 14, 15 };

    Object[] arrayOfArrays = new Object[2];
    arrayOfArrays[0] = givenArrayOne;
    arrayOfArrays[1] = givenArrayTwo;

    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = arrayOfArrays;
    data[1] = "text";
    dataFormat.setObjectData(data);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(2, expectedArray.length);
    assertEquals(Arrays.deepToString(arrayOfArrays), Arrays.deepToString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testArrayOfObjectsWithCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1",
        new org.apache.sqoop.schema.type.Array("array", new FixedPoint("ft"))));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArrayOne = { 11, 12 };
    Object[] givenArrayTwo = { 14, 15 };

    Object[] arrayOfArrays = new Object[2];
    arrayOfArrays[0] = givenArrayOne;
    arrayOfArrays[1] = givenArrayTwo;

    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = arrayOfArrays;
    data[1] = "text";
    dataFormat.setTextData("'[\"[11, 12]\",\"[14, 15]\"]','text'");
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(2, expectedArray.length);
    assertEquals(Arrays.deepToString(arrayOfArrays), Arrays.deepToString(expectedArray));
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testArrayOfObjectsWithCSVTextInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1",
        new org.apache.sqoop.schema.type.Array("array", new FixedPoint("ft"))));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    String input = "'[\"[11, 12]\",\"[14, 15]\"]','text'";
    dataFormat.setTextData(input);
    Object[] expectedArray = (Object[]) dataFormat.getObjectData()[0];
    assertEquals(2, expectedArray.length);
    assertEquals(input, dataFormat.getTextData());
  }

  @Test
  public void testArrayOfObjectsWithObjectArrayInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Array("1",
        new org.apache.sqoop.schema.type.Array("array", new FixedPoint("ft"))));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Object[] givenArrayOne = { 11, 12 };
    Object[] givenArrayTwo = { 14, 15 };

    Object[] arrayOfArrays = new Object[2];
    arrayOfArrays[0] = givenArrayOne;
    arrayOfArrays[1] = givenArrayTwo;

    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = arrayOfArrays;
    data[1] = "text";
    dataFormat.setObjectData(data);
    String expected = "'[\"[11, 12]\",\"[14, 15]\"]','text'";
    assertEquals(expected, dataFormat.getTextData());
  }
  //**************test cases for map**********************

  @Test
  public void testMapWithSimpleValueWithObjectArrayInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Text("value")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Map<Object, Object> map = new HashMap<Object, Object>();
    map.put("testKey", "testValue");
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = map;
    data[1] = "text";
    dataFormat.setObjectData(data);
    @SuppressWarnings("unchecked")
    Map<Object, Object> expectedMap = (Map<Object, Object>) dataFormat.getObjectData()[0];
    assertEquals(map, expectedMap);
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testMapWithComplexIntegerListValueWithObjectArrayInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Array("value",
        new FixedPoint("number"))));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Map<Object, Object> givenMap = new HashMap<Object, Object>();
    List<Integer> intList = new ArrayList<Integer>();
    intList.add(11);
    intList.add(12);
    givenMap.put("testKey", intList);
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenMap;
    data[1] = "text";
    dataFormat.setObjectData(data);
    @SuppressWarnings("unchecked")
    Map<Object, Object> expectedMap = (Map<Object, Object>) dataFormat.getObjectData()[0];
    assertEquals(givenMap.toString(), expectedMap.toString());
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testMapWithComplexStringListValueWithObjectArrayInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Array("value",
        new Text("text"))));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Map<Object, Object> givenMap = new HashMap<Object, Object>();
    List<String> stringList = new ArrayList<String>();
    stringList.add("A");
    stringList.add("A");
    givenMap.put("testKey", stringList);
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenMap;
    data[1] = "text";
    dataFormat.setObjectData(data);
    @SuppressWarnings("unchecked")
    Map<Object, Object> expectedMap = (Map<Object, Object>) dataFormat.getObjectData()[0];
    assertEquals(givenMap.toString(), expectedMap.toString());
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testMapWithComplexMapValueWithObjectArrayInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Array("value",
        new Text("text"))));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Map<Object, Object> givenMap = new HashMap<Object, Object>();
    List<String> stringList = new ArrayList<String>();
    stringList.add("A");
    stringList.add("A");
    Map<String, List<String>> anotherMap = new HashMap<String, List<String>>();
    anotherMap.put("anotherKey", stringList);
    givenMap.put("testKey", anotherMap);
    // create an array inside the object array
    Object[] data = new Object[2];
    data[0] = givenMap;
    data[1] = "text";
    dataFormat.setObjectData(data);
    @SuppressWarnings("unchecked")
    Map<Object, Object> expectedMap = (Map<Object, Object>) dataFormat.getObjectData()[0];
    assertEquals(givenMap.toString(), expectedMap.toString());
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

 @Test
  public void testMapWithCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Text("value")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Map<Object, Object> givenMap = new HashMap<Object, Object>();
    givenMap.put("testKey", "testValue");
    Object[] data = new Object[2];
    data[0] = givenMap;
    data[1] = "text";
    String testData = "'{\"testKey\":\"testValue\"}','text'";
    dataFormat.setTextData(testData);
    @SuppressWarnings("unchecked")
    Map<Object, Object> expectedMap = (Map<Object, Object>) dataFormat.getObjectData()[0];
    assertEquals(givenMap, expectedMap);
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testMapWithComplexValueWithCSVTextInObjectArrayOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Text("value")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Map<Object, Object> givenMap = new HashMap<Object, Object>();
    givenMap.put("testKey", "testValue");
    Object[] data = new Object[2];
    data[0] = givenMap;
    data[1] = "text";
    String testData = "'{\"testKey\":\"testValue\"}','text'";
    dataFormat.setTextData(testData);
    @SuppressWarnings("unchecked")
    Map<Object, Object> expectedMap = (Map<Object, Object>) dataFormat.getObjectData()[0];
    assertEquals(givenMap, expectedMap);
    assertEquals("text", dataFormat.getObjectData()[1]);
  }

  @Test
  public void testMapWithObjectArrayInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Text("value")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    Map<Object, Object> givenMap = new HashMap<Object, Object>();
    givenMap.put("testKey", "testValue");
    Object[] data = new Object[2];
    data[0] = givenMap;
    data[1] = "text";
    String testData = "'{\"testKey\":\"testValue\"}','text'";
    dataFormat.setObjectData(data);
    assertEquals(testData, dataFormat.getTextData());
  }

  @Test
  public void testMapWithCSVTextInCSVTextOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new org.apache.sqoop.schema.type.Map("1", new Text("key"), new Text("value")));
    schema.addColumn(new org.apache.sqoop.schema.type.Text("2"));
    dataFormat.setSchema(schema);
    String testData = "'{\"testKey\":\"testValue\"}','text'";
    dataFormat.setTextData(testData);
    assertEquals(testData, dataFormat.getTextData());
  }
  //**************test cases for schema*******************
  @Test(expected=SqoopException.class)
  public void testEmptySchema() {
    String testData = "10,34,'54','random data'," + getByteFieldString(new byte[] { (byte) -112, (byte) 54})
        + ",'\\n'";
    Schema schema = new Schema("Test");
    dataFormat.setSchema(schema);
    dataFormat.setTextData(testData);

    @SuppressWarnings("unused")
    Object[] out = dataFormat.getObjectData();
  }

  @Test(expected = SqoopException.class)
  public void testNullSchema() {
    dataFormat.setSchema(null);
    @SuppressWarnings("unused")
    Object[] out = dataFormat.getObjectData();
  }

  @Test(expected = SqoopException.class)
  public void testNotSettingSchema() {
    @SuppressWarnings("unused")
    Object[] out = dataFormat.getObjectData();
  }
}
