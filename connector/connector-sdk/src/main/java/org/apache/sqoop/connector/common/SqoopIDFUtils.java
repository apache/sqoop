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

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.CSVIntermediateDataFormatError;
import org.apache.sqoop.schema.type.AbstractComplexListType;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.ColumnType;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Utility methods for connectors to encode data into the sqoop expected formats
 * documented in
 * https://cwiki.apache.org/confluence/display/SQOOP/Intermediate+Data
 * +Format+API
 *
 */

public class SqoopIDFUtils {

  public static final String NULL_VALUE = "NULL";

  // ISO-8859-1 is an 8-bit codec that is supported in every java
  // implementation.
  public static final String BYTE_FIELD_CHARSET = "ISO-8859-1";

  public static final char[] originals = { 0x5C, 0x00, 0x0A, 0x0D, 0x1A, 0x22, 0x27 };

  public static final char CSV_SEPARATOR_CHARACTER = ',';
  public static final char ESCAPE_CHARACTER = '\\';
  public static final char QUOTE_CHARACTER = '\'';

  // string related replacements
  private static final String[] replacements = {
      new String(new char[] { ESCAPE_CHARACTER, '\\' }),
      new String(new char[] { ESCAPE_CHARACTER, '0' }),
      new String(new char[] { ESCAPE_CHARACTER, 'n' }),
      new String(new char[] { ESCAPE_CHARACTER, 'r' }),
      new String(new char[] { ESCAPE_CHARACTER, 'Z' }),
      new String(new char[] { ESCAPE_CHARACTER, '\"' }),
      new String(new char[] { ESCAPE_CHARACTER, '\'' })
      };

  // http://www.joda.org/joda-time/key_format.html provides details on the
  // formatter token
  // can have fraction and or timezone
  public static final DateTimeFormatter dtfWithFractionAndTimeZone = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ");
  public static final DateTimeFormatter dtfWithNoFractionAndTimeZone = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  public static final DateTimeFormatter dtfWithFractionNoTimeZone = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
  public static final DateTimeFormatter dtfWithNoFractionWithTimeZone = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ");

  // only date, no time
  public static final DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd");
  // time with fraction only, no timezone
  public static final DateTimeFormatter tfWithFraction = DateTimeFormat.forPattern("HH:mm:ss.SSSSSS");
  public static final DateTimeFormatter tfWithNoFraction = DateTimeFormat.forPattern("HH:mm:ss");

  static final String[] TRUE_BIT_VALUES = new String[] { "1", "true", "TRUE" };
  static final Set<String> TRUE_BIT_SET = new HashSet<String>(Arrays.asList(TRUE_BIT_VALUES));
  static final String[] FALSE_BIT_VALUES = new String[] { "0", "false", "FALSE" };
  static final Set<String> FALSE_BIT_SET = new HashSet<String>(Arrays.asList(FALSE_BIT_VALUES));

  // ******** Number Column Type utils***********

  public static Object toFixedPoint(String csvString, Column column) {
    Object returnValue;
    Long byteSize = ((FixedPoint) column).getByteSize();
    if (byteSize != null && byteSize <= Integer.SIZE) {
      returnValue = Integer.valueOf(csvString);
    } else {
      returnValue = Long.valueOf(csvString);
    }
    return returnValue;
  }

  public static Object toFloatingPoint(String csvString, Column column) {
    Object returnValue;
    Long byteSize = ((FloatingPoint) column).getByteSize();
    if (byteSize != null && byteSize <= Float.SIZE) {
      returnValue = Float.valueOf(csvString);
    } else {
      returnValue = Double.valueOf(csvString);
    }
    return returnValue;
  }

  public static Object toDecimal(String csvString, Column column) {
    return new BigDecimal(csvString);
  }

  // ********** BIT Column Type utils******************
  public static void encodeToCSVBit(Object[] objectArray, int i) {
    String bitStringValue = objectArray[i].toString();
    if ((TRUE_BIT_SET.contains(bitStringValue)) || (FALSE_BIT_SET.contains(bitStringValue))) {
      objectArray[i] = bitStringValue;
    } else {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0009, " given bit value: "
          + objectArray[i]);
    }
  }

  public static Object toBit(String csvString, Object returnValue) {
    if ((TRUE_BIT_SET.contains(csvString)) || (FALSE_BIT_SET.contains(csvString))) {
      returnValue = TRUE_BIT_SET.contains(csvString);
    } else {
      // throw an exception for any unsupported value for BITs
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0009, " given bit value: " + csvString);
    }
    return returnValue;
  }

  // *********** DATE and TIME Column Type utils **********

  public static void encodeToCSVDate(Object[] objectArray, int i) {
    org.joda.time.LocalDate date = (org.joda.time.LocalDate) objectArray[i];
    objectArray[i] = encloseWithQuote(df.print(date));
  }

  public static void encodeToCSVTime(Object[] objectArray, int i, Column col) {
    if (((org.apache.sqoop.schema.type.Time) col).hasFraction()) {
      objectArray[i] = encloseWithQuote(tfWithFraction.print((org.joda.time.LocalTime) objectArray[i]));
    } else {
      objectArray[i] = encloseWithQuote(tfWithNoFraction.print((org.joda.time.LocalTime) objectArray[i]));
    }
  }

  public static Object toDate(String csvString, Column column) {
    return LocalDate.parse(removeQuotes(csvString));
  }

  public static Object toTime(String csvString, Column column) {
    return LocalTime.parse(removeQuotes(csvString));
  }

  // *********** DATE TIME Column Type utils **********

  public static void encodeToCSVLocalDateTime(Object[] objectArray, int i, Column col, org.joda.time.LocalDateTime localDateTime) {
    org.apache.sqoop.schema.type.DateTime column = (org.apache.sqoop.schema.type.DateTime) col;
    if (column.hasFraction()) {
      objectArray[i] = encloseWithQuote(dtfWithFractionNoTimeZone.print(localDateTime));
    } else {
      objectArray[i] = encloseWithQuote(dtfWithNoFractionAndTimeZone.print(localDateTime));
    }
  }

  public static void encodeToCSVDateTime(Object[] objectArray, int i, Column col, org.joda.time.DateTime dateTime) {
    org.apache.sqoop.schema.type.DateTime column = (org.apache.sqoop.schema.type.DateTime) col;
    if (column.hasFraction() && column.hasTimezone()) {
      objectArray[i] = encloseWithQuote(dtfWithFractionAndTimeZone.print(dateTime));
    } else if (column.hasFraction() && !column.hasTimezone()) {
      objectArray[i] = encloseWithQuote(dtfWithFractionNoTimeZone.print(dateTime));
    } else if (column.hasTimezone()) {
      objectArray[i] = encloseWithQuote(dtfWithNoFractionWithTimeZone.print(dateTime));
    } else {
      objectArray[i] = encloseWithQuote(dtfWithNoFractionAndTimeZone.print(dateTime));
    }
  }

  public static Object toDateTime(String fieldString, Column column) {
    Object returnValue;
    String dateTime = removeQuotes(fieldString);
    org.apache.sqoop.schema.type.DateTime col = ((org.apache.sqoop.schema.type.DateTime) column);
    if (col.hasFraction() && col.hasTimezone()) {
      // After calling withOffsetParsed method, a string
      // '2004-06-09T10:20:30-08:00' will create a datetime with a zone of
      // -08:00 (a fixed zone, with no daylight savings rules)
      returnValue = dtfWithFractionAndTimeZone.withOffsetParsed().parseDateTime(dateTime);
    } else if (col.hasFraction() && !col.hasTimezone()) {
      // we use local date time explicitly to not include the timezone
      returnValue = dtfWithFractionNoTimeZone.parseLocalDateTime(dateTime);
    } else if (col.hasTimezone()) {
      returnValue = dtfWithNoFractionWithTimeZone.withOffsetParsed().parseDateTime(dateTime);
    } else {
      // we use local date time explicitly to not include the timezone
      returnValue = dtfWithNoFractionAndTimeZone.parseLocalDateTime(dateTime);
    }
    return returnValue;
  }

  // ************ MAP Column Type utils*********

  @SuppressWarnings("unchecked")
  public static String encodeToCSVMap(Map<Object, Object> map, Column column) {
    JSONObject object = new JSONObject();
    object.putAll(map);
    return encloseWithQuote(object.toJSONString());
  }

  public static Map<Object, Object> toMap(String csvString) {

    JSONObject object = null;
    try {
      object = (JSONObject) new JSONParser().parse(removeQuotes(csvString));
    } catch (org.json.simple.parser.ParseException e) {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0008, e);
    }
    if (object != null) {
      return toMap(object);
    }
    return null;
  }

  private static List<Object> toList(JSONArray array) {
    List<Object> list = new ArrayList<Object>();
    for (int i = 0; i < array.size(); i++) {
      Object value = array.get(i);
      if (value instanceof JSONArray) {
        value = toList((JSONArray) value);
      }

      else if (value instanceof JSONObject) {
        value = toMap((JSONObject) value);
      }
      list.add(value);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private static Map<Object, Object> toMap(JSONObject object) {
    Map<Object, Object> elementMap = new HashMap<Object, Object>();
    Set<Map.Entry<Object, Object>> entries = object.entrySet();
    for (Map.Entry<Object, Object> entry : entries) {
      Object value = entry.getValue();

      if (value instanceof JSONArray) {
        value = toList((JSONArray) value);
      }

      else if (value instanceof JSONObject) {
        value = toMap((JSONObject) value);
      }
      elementMap.put(entry.getKey(), value);
    }
    return elementMap;
  }

  // ************ LIST Column Type utils*********

  @SuppressWarnings("unchecked")
  public static String encodeToCSVList(Object[] list, AbstractComplexListType column) {
    List<Object> elementList = new ArrayList<Object>();
    for (int n = 0; n < list.length; n++) {
      Column listType = ((AbstractComplexListType) column).getListType();
      if (isColumnListType(listType)) {
        Object[] listElements = (Object[]) list[n];
        elementList.add((Arrays.deepToString(listElements)));
      } else {
        elementList.add(list[n]);
      }
    }
    JSONArray array = new JSONArray();
    array.addAll(elementList);
    return encloseWithQuote(array.toJSONString());
  }

  public static Object[] toList(String csvString) {

    JSONArray array = null;
    try {
      array = (JSONArray) new JSONParser().parse(removeQuotes(csvString));
    } catch (org.json.simple.parser.ParseException e) {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0008, e);
    }
    if (array != null) {
      return array.toArray();
    }
    return null;
  }

  // ************ TEXT Column Type utils*********

  private static String getRegExp(char character) {
    return getRegExp(String.valueOf(character));
  }

  private static String getRegExp(String string) {
    return string.replaceAll("\\\\", Matcher.quoteReplacement("\\\\"));
  }

  public static String encodeToCSVString(String string) {
    int j = 0;
    String replacement = string;
    try {
      for (j = 0; j < replacements.length; j++) {
        replacement = replacement.replaceAll(getRegExp(originals[j]), Matcher.quoteReplacement(replacements[j]));
      }
    } catch (Exception e) {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0002, string + "  " + replacement
          + "  " + String.valueOf(j) + "  " + e.getMessage());
    }
    return encloseWithQuote(replacement);
  }

  public static String toText(String csvString) {
    // Remove the trailing and starting quotes.
    csvString = removeQuotes(csvString);
    int j = 0;
    try {
      for (j = 0; j < replacements.length; j++) {
        csvString = csvString.replaceAll(getRegExp(replacements[j]), Matcher.quoteReplacement(String.valueOf(originals[j])));
      }
    } catch (Exception e) {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0003, csvString + "  "
          + String.valueOf(j) + e.getMessage());
    }

    return csvString;
  }

  // ************ BINARY Column type utils*********

  public static String encodeToCSVByteArray(byte[] bytes) {
    try {
      return encodeToCSVString(new String(bytes, BYTE_FIELD_CHARSET));
    } catch (UnsupportedEncodingException e) {
      // We should never hit this case.
      // This character set should be distributed with Java.
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0001, "The character set "
          + BYTE_FIELD_CHARSET + " is not available.");
    }
  }

  public static byte[] toByteArray(String csvString) {
    // Always encoded in BYTE_FIELD_CHARSET.
    try {
      return toText(csvString).getBytes(BYTE_FIELD_CHARSET);
    } catch (UnsupportedEncodingException e) {
      // Should never hit this case.
      // This character set should be distributed with Java.
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0001, "The character set "
          + BYTE_FIELD_CHARSET + " is not available.");
    }
  }

  // *********** SQOOP CSV standard encoding utils********************

  public static String encloseWithQuote(String string) {
    StringBuilder builder = new StringBuilder();
    builder.append(QUOTE_CHARACTER).append(string).append(QUOTE_CHARACTER);
    return builder.toString();
  }

  public static String removeQuotes(String string) {
    // validate that the string has quotes
    if (string.startsWith(String.valueOf(QUOTE_CHARACTER)) && string.endsWith(String.valueOf(QUOTE_CHARACTER))) {
      return string.substring(1, string.length() - 1);
    }
    return string;
  }

  // ********* utility methods for column type classification ***********
  public static boolean isColumnListType(Column listType) {
    return listType.getType().equals(ColumnType.ARRAY) || listType.getType().equals(ColumnType.SET);
  }

  public static boolean isColumnStringType(Column stringType) {
    return stringType.getType().equals(ColumnType.TEXT) || stringType.getType().equals(ColumnType.ENUM);
  }

}
