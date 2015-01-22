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

import static org.apache.sqoop.connector.common.SqoopIDFUtils.NULL_VALUE;
import static org.apache.sqoop.connector.common.TestSqoopIDFUtils.getByteFieldString;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.common.SqoopIDFUtils;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Array;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.Text;
import org.joda.time.LocalDateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestJSONIntermediateDataFormat {

  private JSONIntermediateDataFormat dataFormat;
  private final static String csvArray = "'[[11,11],[14,15]]'";
  private final static String map = "'{\"testKey\":\"testValue\"}'";
  private final static String csvSet = "'[[11,12],[14,15]]'";
  private final static String date = "'2014-10-01'";
  private final static String dateTime = "'2014-10-01 12:00:00.000'";
  private final static String time = "'12:59:59'";

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    createJSONIDF();
  }

  private void createJSONIDF() {
    Schema schema = new Schema("test");
    schema
        .addColumn(new FixedPoint("1", 8L, true))
        .addColumn(new FixedPoint("2", 4L, true))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"))
        .addColumn(new org.apache.sqoop.schema.type.Enum("7"))
        .addColumn(new Array("8", new Array("array", new FixedPoint("ft", 2L, false))))
        .addColumn(new org.apache.sqoop.schema.type.Map("9", new Text("t1"), new Text("t2")))
        .addColumn(new Bit("10"))
        .addColumn(new org.apache.sqoop.schema.type.DateTime("11", true, false))
        .addColumn(new org.apache.sqoop.schema.type.Time("12", false))
        .addColumn(new org.apache.sqoop.schema.type.Date("13"))
        .addColumn(new org.apache.sqoop.schema.type.FloatingPoint("14", 4L))
        .addColumn(
            new org.apache.sqoop.schema.type.Set("15", new Array("set", new FixedPoint("ftw", 2L, false))));
    dataFormat = new JSONIntermediateDataFormat(schema);
  }

  /**
   * setCSVGetData setCSVGetObjectArray setCSVGetCSV
   */
  @Test
  public void testInputAsCSVTextInAndDataOut() {

    String csvText = "10,34,'54','random data',"
        + getByteFieldString(new byte[] { (byte) -112, (byte) 54 }) + ",'" + String.valueOf(0x0A)
        + "','ENUM'," + csvArray + "," + map + ",true," + dateTime + "," + time + "," + date
        + ",13.44," + csvSet;
    dataFormat.setCSVTextData(csvText);
    String jsonExpected = "{\"15\":[[11,12],[14,15]],\"13\":\"2014-10-01\",\"14\":13.44,\"11\":\"2014-10-01 12:00:00.000\","
        + "\"12\":\"12:59:59\",\"3\":\"54\",\"2\":34,\"1\":10,\"10\":true,\"7\":\"ENUM\",\"6\":\"10\",\"5\":\"kDY=\",\"4\":\"random data\","
        + "\"9\":{\"testKey\":\"testValue\"},\"8\":[[11,11],[14,15]]}";
    assertEquals(jsonExpected, dataFormat.getData().toJSONString());
  }

  @Test
  public void testInputAsCSVTextInAndObjectArrayOut() {
    String csvText = "10,34,'54','random data',"
        + getByteFieldString(new byte[] { (byte) -112, (byte) 54 }) + ",'" + String.valueOf(0x0A)
        + "','ENUM'," + csvArray + "," + map + ",true," + dateTime + "," + time + "," + date
        + ",13.44," + csvSet;
    dataFormat.setCSVTextData(csvText);
    assertEquals(dataFormat.getObjectData().length, 15);
    assertObjectArray();

  }

  private void assertObjectArray() {
    Object[] out = dataFormat.getObjectData();
    assertEquals(10L, out[0]);
    assertEquals(34, out[1]);
    assertEquals("54", out[2]);
    assertEquals("random data", out[3]);
    assertEquals(-112, ((byte[]) out[4])[0]);
    assertEquals(54, ((byte[]) out[4])[1]);
    assertEquals("10", out[5]);
    assertEquals("ENUM", out[6]);

    Object[] givenArrayOne = new Object[2];
    givenArrayOne[0] = 11;
    givenArrayOne[1] = 11;
    Object[] givenArrayTwo = new Object[2];
    givenArrayTwo[0] = 14;
    givenArrayTwo[1] = 15;
    Object[] arrayOfArrays = new Object[2];
    arrayOfArrays[0] = givenArrayOne;
    arrayOfArrays[1] = givenArrayTwo;
    Map<Object, Object> map = new HashMap<Object, Object>();
    map.put("testKey", "testValue");
    // no time zone
    LocalDateTime dateTime = new org.joda.time.LocalDateTime(2014, 10, 01, 12, 0, 0);
    org.joda.time.LocalTime time = new org.joda.time.LocalTime(12, 59, 59);
    org.joda.time.LocalDate date = new org.joda.time.LocalDate(2014, 10, 01);
    Object[] set0 = new Object[2];
    set0[0] = 11;
    set0[1] = 12;
    Object[] set1 = new Object[2];
    set1[0] = 14;
    set1[1] = 15;
    Object[] set = new Object[2];
    set[0] = set0;
    set[1] = set1;
    out[14] = set;
    assertEquals(arrayOfArrays.length, 2);
    assertEquals(Arrays.deepToString(arrayOfArrays), Arrays.deepToString((Object[]) out[7]));
    assertEquals(map, out[8]);
    assertEquals(true, out[9]);
    assertEquals(dateTime, out[10]);
    assertEquals(time, out[11]);
    assertEquals(date, out[12]);
    assertEquals(13.44f, out[13]);
    assertEquals(set.length, 2);
    assertEquals(Arrays.deepToString(set), Arrays.deepToString((Object[]) out[14]));

  }

  @Test
  public void testInputAsCSVTextInCSVTextOut() {
    String csvText = "10,34,'54','random data',"
        + getByteFieldString(new byte[] { (byte) -112, (byte) 54 }) + ",'" + String.valueOf(0x0A)
        + "','ENUM'," + csvArray + "," + map + ",true," + dateTime + "," + time + "," + date
        + ",13.44," + csvSet;
    dataFormat.setCSVTextData(csvText);
    assertEquals(csvText, dataFormat.getCSVTextData());
  }

  @SuppressWarnings("unchecked")
  private JSONObject createJSONObject() {
    JSONObject json = new JSONObject();
    json.put("1", 10L);
    json.put("2", 34);
    json.put("3", "54");
    json.put("4", "random data");
    json.put(
        "5",
        org.apache.commons.codec.binary.Base64.encodeBase64String(new byte[] { (byte) -112,
            (byte) 54 }));
    json.put("6", String.valueOf(0x0A));
    json.put("7", "ENUM");
    JSONArray givenArrayOne = new JSONArray();
    givenArrayOne.add(11);
    givenArrayOne.add(11);
    JSONArray givenArrayTwo = new JSONArray();
    givenArrayTwo.add(14);
    givenArrayTwo.add(15);

    JSONArray arrayOfArrays = new JSONArray();
    arrayOfArrays.add(givenArrayOne);
    arrayOfArrays.add(givenArrayTwo);

    JSONObject map = new JSONObject();
    map.put("testKey", "testValue");

    json.put("8", arrayOfArrays);
    json.put("9", map);
    json.put("10", true);

    // expect dates as strings
    json.put("11", SqoopIDFUtils.removeQuotes(dateTime));
    json.put("12", SqoopIDFUtils.removeQuotes(time));
    json.put("13", SqoopIDFUtils.removeQuotes(date));
    json.put("14", 13.44f);

    JSONArray givenSetOne = new JSONArray();
    givenSetOne.add(11);
    givenSetOne.add(12);
    JSONArray givenSetTwo = new JSONArray();
    givenSetTwo.add(14);
    givenSetTwo.add(15);
    JSONArray arrayOfSet = new JSONArray();
    arrayOfSet.add(givenSetOne);
    arrayOfSet.add(givenSetTwo);

    json.put("15", arrayOfSet);
    return json;
  }

  /**
   * setDataGetCSV setDataGetObjectArray setDataGetData
   */
  @Test
  public void testInputAsDataInAndCSVOut() {

    String csvExpected = "10,34,'54','random data',"
        + getByteFieldString(new byte[] { (byte) -112, (byte) 54 }) + ",'" + String.valueOf(0x0A)
        + "','ENUM'," + csvArray + "," + map + ",true," + dateTime + "," + time + "," + date
        + ",13.44," + csvSet;
    dataFormat.setData(createJSONObject());
    assertEquals(csvExpected, dataFormat.getCSVTextData());
  }

  @Test
  public void testInputAsDataInAndObjectArrayOut() {
    JSONObject json = createJSONObject();
    dataFormat.setData(json);
    assertObjectArray();
  }

  @Test
  public void testInputAsDataInAndDataOut() {
    JSONObject json = createJSONObject();
    dataFormat.setData(json);
    assertEquals(json, dataFormat.getData());
  }

  private Object[] createObjectArray() {
    Object[] out = new Object[15];
    out[0] = 10L;
    out[1] = 34;
    out[2] = "54";
    out[3] = "random data";
    out[4] = new byte[] { (byte) -112, (byte) 54 };
    out[5] = String.valueOf(0x0A);
    out[6] = "ENUM";

    Object[] givenArrayOne = new Object[2];
    givenArrayOne[0] = 11;
    givenArrayOne[1] = 11;
    Object[] givenArrayTwo = new Object[2];
    givenArrayTwo[0] = 14;
    givenArrayTwo[1] = 15;

    Object[] arrayOfArrays = new Object[2];
    arrayOfArrays[0] = givenArrayOne;
    arrayOfArrays[1] = givenArrayTwo;

    Map<Object, Object> map = new HashMap<Object, Object>();
    map.put("testKey", "testValue");

    out[7] = arrayOfArrays;
    out[8] = map;
    out[9] = true;
    org.joda.time.DateTime dateTime = new org.joda.time.DateTime(2014, 10, 01, 12, 0, 0, 0);
    out[10] = dateTime;
    org.joda.time.LocalTime time = new org.joda.time.LocalTime(12, 59, 59);

    out[11] = time;
    org.joda.time.LocalDate date = new org.joda.time.LocalDate(2014, 10, 01);

    out[12] = date;

    out[13] = 13.44f;
    Object[] set0 = new Object[2];
    set0[0] = 11;
    set0[1] = 12;
    Object[] set1 = new Object[2];
    set1[0] = 14;
    set1[1] = 15;

    Object[] set = new Object[2];
    set[0] = set0;
    set[1] = set1;
    out[14] = set;
    return out;
  }

  /**
   * setObjectArrayGetData setObjectArrayGetCSV setObjectArrayGetObjectArray
   */
  @Test
  public void testInputAsObjectArrayInAndDataOut() {

    Object[] out = createObjectArray();
    dataFormat.setObjectData(out);
    JSONObject json = createJSONObject();
    assertEquals(json, dataFormat.getData());
  }

  @Test
  public void testInputAsObjectArrayInAndCSVOut() {
    Object[] out = createObjectArray();
    dataFormat.setObjectData(out);
    String csvText = "10,34,'54','random data',"
        + getByteFieldString(new byte[] { (byte) -112, (byte) 54 }) + ",'" + String.valueOf(0x0A)
        + "','ENUM'," + csvArray + "," + map + ",true," + dateTime + "," + time + "," + date
        + ",13.44," + csvSet;
    assertEquals(csvText, dataFormat.getCSVTextData());
  }

  @Test
  public void testInputAsObjectArrayInAndObjectArrayOut() {
    Object[] out = createObjectArray();
    dataFormat.setObjectData(out);
    assertObjectArray();
  }

  // **************test cases for empty and null schema*******************
  @Test(expectedExceptions = SqoopException.class)
  public void testEmptySchema() {
    String testData = "10,34,'54','random data',"
        + getByteFieldString(new byte[] { (byte) -112, (byte) 54 }) + ",'\\n'";
    // no coumns
    Schema schema = new Schema("Test");
    dataFormat = new JSONIntermediateDataFormat(schema);
    dataFormat.setCSVTextData(testData);

    @SuppressWarnings("unused")
    Object[] out = dataFormat.getObjectData();
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNullSchema() {
    dataFormat = new JSONIntermediateDataFormat(null);
    dataFormat.getObjectData();
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNotSettingSchemaAndGetObjectData() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.getObjectData();
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNotSettingSchemaAndGetData() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.getData();
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNotSettingSchemaAndGetCSVData() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.getCSVTextData();
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNotSettingSchemaAndSetObjectData() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.setObjectData(null);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNotSettingSchemaAndSetData() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.setData(null);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNotSettingSchemaAndSetCSVData() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.setCSVTextData(null);
  }

  // **************test cases for null input*******************

  @Test
  public void testNullInputAsCSVTextInObjectArrayOut() {

    dataFormat.setCSVTextData(null);
    Object[] out = dataFormat.getObjectData();
    assertNull(out);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testEmptyInputAsCSVTextInObjectArrayOut() {
    dataFormat.setCSVTextData("");
    dataFormat.getObjectData();
  }

  @Test
  public void testNullValueAsObjectArrayInAndCSVTextOut() {

    Object[] in = { null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null };
    dataFormat.setObjectData(in);

    String csvText = dataFormat.getCSVTextData();
    String[] textValues = csvText.split(",");
    assertEquals(15, textValues.length);
    for (String text : textValues) {
      assertEquals(text, NULL_VALUE);
    }
  }

  @Test
  public void testNullValueAsObjectArrayInAndObjectArrayOut() {
    Object[] in = { null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null };
    dataFormat.setObjectData(in);

    Object[] out = dataFormat.getObjectData();
    assertEquals(15, out.length);
    for (Object obj : out) {
      assertEquals(obj, null);
    }
  }

  @Test
  public void testNullValueAsCSVTextInAndObjectArrayOut() {
    String[] test = { "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
        "NULL", "NULL", "NULL", "NULL", "NULL", "NULL" };
    dataFormat.setCSVTextData(StringUtils.join(test, ","));
    Object[] out = dataFormat.getObjectData();
    assertEquals(15, out.length);
    for (Object obj : out) {
      assertEquals(obj, null);
    }
  }

  @Test
  public void testNullValueAsCSVTextInAndCSVTextOut() {

    String[] test = { "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
        "NULL", "NULL", "NULL", "NULL", "NULL", "NULL" };
    dataFormat.setCSVTextData(StringUtils.join(test, ","));

    String csvText = dataFormat.getCSVTextData();
    String[] textValues = csvText.split(",");
    assertEquals(15, textValues.length);
    for (String text : textValues) {
      assertEquals(text, NULL_VALUE);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNullValueAsDataInAndCSVTextOut() {

    JSONObject json = new JSONObject();
    for (int i = 1; i <= 15; i++) {
      json.put(String.valueOf(i), null);
    }
    dataFormat.setData(json);

    String csvText = dataFormat.getCSVTextData();
    String[] textValues = csvText.split(",");
    assertEquals(15, textValues.length);
    for (String text : textValues) {
      assertEquals(text, NULL_VALUE);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNullValueAsDataInAndObjectArrayOut() {

    JSONObject json = new JSONObject();
    for (int i = 1; i <= 15; i++) {
      json.put(String.valueOf(i), null);
    }
    dataFormat.setData(json);

    Object[] out = dataFormat.getObjectData();
    assertEquals(15, out.length);
    for (Object obj : out) {
      assertEquals(obj, null);
    }

  }

  @Test(expectedExceptions = SqoopException.class)
  public void testSchemaNotNullableWithObjectArray() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.setSchema(new Schema("Test").addColumn(new Text("t").setNullable(false)));
    Object[] out = new Object[1];
    out[0] = null;
    dataFormat.setObjectData(out);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testSchemaNotNullableWithCSV() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.setSchema(new Schema("Test").addColumn(new Text("t").setNullable(false)));
    dataFormat.setCSVTextData(NULL_VALUE);
  }

  @SuppressWarnings("unchecked")
  // no validation happens when the setJSON and getJSON is used
  @Test
  public void testSchemaNotNullableWithJSON() {
    dataFormat = new JSONIntermediateDataFormat();
    dataFormat.setSchema(new Schema("Test").addColumn(new Text("t").setNullable(false)));
    JSONObject json = new JSONObject();
    json.put("test", null);
    dataFormat.setData(json);
    dataFormat.getData();
  }

}
