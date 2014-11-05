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
import java.util.Arrays;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.Text;
import org.junit.Before;
import org.junit.Test;

public class TestCSVIntermediateDataFormat {

  private final String BYTE_FIELD_ENCODING = "ISO-8859-1";

  private IntermediateDataFormat<?> data;

  @Before
  public void setUp() {
    data = new CSVIntermediateDataFormat();
  }

  private String getByteFieldString(byte[] byteFieldData) {
    try {
      return new StringBuilder("'").append(new String(byteFieldData, BYTE_FIELD_ENCODING)).append("'").toString();
    } catch(UnsupportedEncodingException e) {
      // Should never get to this point because ISO-8859-1 is a standard codec.
      return null;
    }
  }

  @Test
  public void testStringInStringOut() {
    String testData = "10,34,'54','random data'," + getByteFieldString(new byte[] { (byte) -112, (byte) 54})
      + ",'" + String.valueOf(0x0A) + "'";
    data.setTextData(testData);
    assertEquals(testData, data.getTextData());
  }

  @Test
  public void testNullStringInObjectOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));
    data.setSchema(schema);
    data.setTextData(null);

    Object[] out = data.getObjectData();

    assertNull(out);
  }

  @Test(expected=SqoopException.class)
  public void testEmptyStringInObjectOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));
    data.setSchema(schema);
    data.setTextData("");

    data.getObjectData();
  }

  @Test
  public void testStringInObjectOut() {

    //byte[0] = -112, byte[1] = 54 - 2's complements
    String testData = "10,34,'54','random data'," + getByteFieldString(new byte[] { (byte) -112, (byte) 54})
      + ",'\\n'";
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));

    data.setSchema(schema);
    data.setTextData(testData);

    Object[] out = data.getObjectData();

    assertEquals(new Long(10),out[0]);
    assertEquals(new Long(34),out[1]);
    assertEquals("54",out[2]);
    assertEquals("random data",out[3]);
    assertEquals(-112, ((byte[]) out[4])[0]);
    assertEquals(54, ((byte[])out[4])[1]);
    assertEquals("\n", out[5].toString());
  }

  @Test
  public void testObjectInStringOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));
    data.setSchema(schema);

    byte[] byteFieldData = new byte[] { (byte) 0x0D, (byte) -112, (byte) 54};
    Object[] in = new Object[6];
    in[0] = new Long(10);
    in[1] = new Long(34);
    in[2] = "54";
    in[3] = "random data";
    in[4] = byteFieldData;
    in[5] = new String(new char[] { 0x0A });

    data.setObjectData(in);

    //byte[0] = \r byte[1] = -112, byte[1] = 54 - 2's complements
    String testData = "10,34,'54','random data'," +
        getByteFieldString(byteFieldData).replaceAll("\r", "\\\\r") + ",'\\n'";
    assertEquals(testData, data.getTextData());
  }

  @Test
  public void testObjectInObjectOut() {
    //Test escapable sequences too.
    //byte[0] = -112, byte[1] = 54 - 2's complements
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));
    data.setSchema(schema);

    Object[] in = new Object[6];
    in[0] = new Long(10);
    in[1] = new Long(34);
    in[2] = "54";
    in[3] = "random data";
    in[4] = new byte[] { (byte) -112, (byte) 54};
    in[5] = new String(new char[] { 0x0A });
    Object[] inCopy = new Object[6];
    System.arraycopy(in,0,inCopy,0,in.length);

    // Modifies the input array, so we use the copy to confirm
    data.setObjectData(in);

    assertTrue(Arrays.deepEquals(inCopy, data.getObjectData()));
  }

  @Test
  public void testObjectWithNullInStringOut() {
    Schema schema = new Schema("test");
    schema.addColumn(new FixedPoint("1"))
        .addColumn(new FixedPoint("2"))
        .addColumn(new Text("3"))
        .addColumn(new Text("4"))
        .addColumn(new Binary("5"))
        .addColumn(new Text("6"));
    data.setSchema(schema);

    byte[] byteFieldData = new byte[] { (byte) 0x0D, (byte) -112, (byte) 54};
    Object[] in = new Object[6];
    in[0] = new Long(10);
    in[1] = new Long(34);
    in[2] = null;
    in[3] = "random data";
    in[4] = byteFieldData;
    in[5] = new String(new char[] { 0x0A });

    data.setObjectData(in);

    //byte[0] = \r byte[1] = -112, byte[1] = 54 - 2's complements
    String testData = "10,34,NULL,'random data'," +
        getByteFieldString(byteFieldData).replaceAll("\r", "\\\\r") + ",'\\n'";
    assertEquals(testData, data.getTextData());
  }

  @Test
  public void testStringFullRangeOfCharacters() {
    Schema schema = new Schema("test");
    schema.addColumn(new Text("1"));

    data.setSchema(schema);

    char[] allCharArr = new char[256];
    for(int i = 0; i < allCharArr.length; ++i) {
      allCharArr[i] = (char)i;
    }
    String strData = new String(allCharArr);

    Object[] in = {strData};
    Object[] inCopy = new Object[1];
    System.arraycopy(in, 0, inCopy, 0, in.length);

    // Modifies the input array, so we use the copy to confirm
    data.setObjectData(in);

    assertEquals(strData, data.getObjectData()[0]);
    assertTrue(Arrays.deepEquals(inCopy, data.getObjectData()));
  }

  @Test
  public void testByteArrayFullRangeOfCharacters() {
    Schema schema = new Schema("test");
    schema.addColumn(new Binary("1"));
    data.setSchema(schema);

    byte[] allCharByteArr = new byte[256];
    for (int i = 0; i < allCharByteArr.length; ++i) {
      allCharByteArr[i] = (byte) i;
    }

    Object[] in = {allCharByteArr};
    Object[] inCopy = new Object[1];
    System.arraycopy(in, 0, inCopy, 0, in.length);

    // Modifies the input array, so we use the copy to confirm
    data.setObjectData(in);
    assertTrue(Arrays.deepEquals(inCopy, data.getObjectData()));
  }

  @Test
  public void testDate() {
    Schema schema = new Schema("test");
    schema.addColumn(new Date("1"));
    data.setSchema(schema);

    data.setTextData("2014-10-01");
    assertEquals("2014-10-01", data.getObjectData()[0].toString());
  }

  @Test
  public void testDateTime() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    data.setSchema(schema);

    for (String dateTime : new String[]{
        "2014-10-01T12:00:00",
        "2014-10-01T12:00:00.000"
    }) {
      data.setTextData(dateTime);
      assertEquals("2014-10-01T12:00:00.000", data.getObjectData()[0].toString());
    }
  }

  /**
   * In ISO8601 "T" is used as date-time separator. Unfortunately in the real
   * world, database (confirmed with mysql and postgres) might return a datatime
   * string with a space as separator. The test case intends to check, whether
   * such datatime string can be handled expectedly.
   */
  @Test
  public void testDateTimeISO8601Alternative() {
    Schema schema = new Schema("test");
    schema.addColumn(new DateTime("1"));
    data.setSchema(schema);

    for (String dateTime : new String[]{
        "2014-10-01 12:00:00",
        "2014-10-01 12:00:00.000"
    }) {
      data.setTextData(dateTime);
      assertEquals("2014-10-01T12:00:00.000", data.getObjectData()[0].toString());
    }
  }

  @Test
  public void testBit() {
    Schema schema = new Schema("test");
    schema.addColumn(new Bit("1"));
    data.setSchema(schema);

    for (String trueBit : new String[]{
        "true", "TRUE", "1"
    }) {
      data.setTextData(trueBit);
      assertTrue((Boolean) data.getObjectData()[0]);
    }

    for (String falseBit : new String[]{
        "false", "FALSE", "0"
    }) {
      data.setTextData(falseBit);
      assertFalse((Boolean) data.getObjectData()[0]);
    }
  }

  @Test(expected=SqoopException.class)
  public void testEmptySchema() {
    String testData = "10,34,'54','random data'," + getByteFieldString(new byte[] { (byte) -112, (byte) 54})
        + ",'\\n'";
    Schema schema = new Schema("Test");
    data.setSchema(schema);
    data.setTextData(testData);

    Object[] out = data.getObjectData();
  }
}
