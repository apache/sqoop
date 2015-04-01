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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.CSVIntermediateDataFormatError;
import org.apache.sqoop.error.code.IntermediateDataFormatError;
import org.apache.sqoop.schema.Schema;
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
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

/**
 * Utility methods for connectors to encode data into the sqoop expected formats
 * documented in
 * https://cwiki.apache.org/confluence/display/SQOOP/Intermediate+Data
 * +Format+API
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SqoopIDFUtils {

  public static final String NULL_VALUE = "NULL";

  // ISO-8859-1 is an 8-bit codec that is supported in every java
  // implementation.
  public static final String BYTE_FIELD_CHARSET = "ISO-8859-1";

  public static final Map<Character, String> ORIGINALS = new TreeMap<Character, String>();

  public static final char CSV_SEPARATOR_CHARACTER = ',';
  public static final char ESCAPE_CHARACTER = '\\';
  public static final char QUOTE_CHARACTER = '\'';

  private static final Map<Character, Character> REPLACEMENTS = new TreeMap<Character, Character>();

  static {
    ORIGINALS.put(new Character((char)0x00), new String(new char[] { ESCAPE_CHARACTER, '0' }));
    ORIGINALS.put(new Character((char)0x0A), new String(new char[] { ESCAPE_CHARACTER, 'n' }));
    ORIGINALS.put(new Character((char)0x0D), new String(new char[] { ESCAPE_CHARACTER, 'r' }));
    ORIGINALS.put(new Character((char)0x1A), new String(new char[] { ESCAPE_CHARACTER, 'Z' }));
    ORIGINALS.put(new Character((char)0x22), new String(new char[] { ESCAPE_CHARACTER, '"' }));
    ORIGINALS.put(new Character((char)0x27), new String(new char[] { ESCAPE_CHARACTER, '\'' }));

    REPLACEMENTS.put('0', new Character((char)0x00));
    REPLACEMENTS.put('n', new Character((char)0x0A));
    REPLACEMENTS.put('r', new Character((char)0x0D));
    REPLACEMENTS.put('Z', new Character((char)0x1A));
    REPLACEMENTS.put('"', new Character((char)0x22));
    REPLACEMENTS.put('\'', new Character((char)0x27));
  }

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
  public static final DateTimeFormatter tfWithFraction = DateTimeFormat.forPattern("HH:mm:ss.SSS");
  public static final DateTimeFormatter tfWithNoFraction = DateTimeFormat.forPattern("HH:mm:ss");

  public static final String[] TRUE_BIT_VALUES = new String[] { "1", "true", "TRUE" };
  public static final Set<String> TRUE_BIT_SET = new HashSet<String>(Arrays.asList(TRUE_BIT_VALUES));
  public static final String[] FALSE_BIT_VALUES = new String[] { "0", "false", "FALSE" };
  public static final Set<String> FALSE_BIT_SET = new HashSet<String>(Arrays.asList(FALSE_BIT_VALUES));

  // ******** Number Column Type utils***********

  public static boolean isInteger(Column column) {
    Long byteSize = ((FixedPoint) column).getByteSize();
    Boolean signed = ((FixedPoint) column).isSigned();

    if (byteSize == null) {
      return false;
    }
    if (signed != null && !signed) {
      byteSize *= 2;
    }
    return byteSize <= (Integer.SIZE / Byte.SIZE);
  }

  public static String toCSVFixedPoint(Object obj, Column column) {
    if (isInteger(column)) {
      if (obj instanceof Number) {
        return new Integer(((Number)obj).intValue()).toString();
      } else {
        return new Integer(obj.toString()).toString();
      }
    } else {
      if (obj instanceof Number) {
        return new Long(((Number)obj).longValue()).toString();
      } else {
        return new Long(obj.toString()).toString();
      }
    }
  }

  public static Object toFixedPoint(String csvString, Column column) {
    Object returnValue;
    if (isInteger(column)) {
      returnValue = Integer.valueOf(csvString);
    } else {
      returnValue = Long.valueOf(csvString);
    }
    return returnValue;
  }

  public static String toCSVFloatingPoint(Object obj, Column column) {
    Long byteSize = ((FloatingPoint) column).getByteSize();
    if (byteSize != null && byteSize <= (Float.SIZE / Byte.SIZE)) {
      return ((Float) obj).toString();
    } else {
      return ((Double) obj).toString();
    }
  }

  public static Object toFloatingPoint(String csvString, Column column) {
    Object returnValue;
    Long byteSize = ((FloatingPoint) column).getByteSize();
    if (byteSize != null && byteSize <= (Float.SIZE / Byte.SIZE)) {
      returnValue = Float.valueOf(csvString);
    } else {
      returnValue = Double.valueOf(csvString);
    }
    return returnValue;
  }

  public static String toCSVDecimal(Object obj) {
    return ((BigDecimal) obj).toString();
  }

  public static Object toDecimal(String csvString, Column column) {
    Integer precision = ((org.apache.sqoop.schema.type.Decimal) column).getPrecision();
    Integer scale = ((org.apache.sqoop.schema.type.Decimal) column).getScale();
    BigDecimal bd = null;
    if (precision != null) {
      MathContext mc = new MathContext(precision);
      bd = new BigDecimal(csvString, mc);
    } else {
      bd = new BigDecimal(csvString);
    }
    if (scale != null) {
      // we have decided to use the default MathContext DEFAULT_ROUNDINGMODE
      // which is RoundingMode.HALF_UP,
      // we are aware that there may be some loss
      bd.setScale(scale, RoundingMode.HALF_UP);
    }
    return bd;
  }

  // ********** BIT Column Type utils******************
  public static String toCSVBit(Object obj) {
    String bitStringValue = obj.toString();
    if ((TRUE_BIT_SET.contains(bitStringValue)) || (FALSE_BIT_SET.contains(bitStringValue))) {
      return bitStringValue;
    } else {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0005, " given bit value: "
          + bitStringValue);
    }
  }

  public static Object toBit(String csvString) {
    if ((TRUE_BIT_SET.contains(csvString)) || (FALSE_BIT_SET.contains(csvString))) {
      return TRUE_BIT_SET.contains(csvString);
    } else {
      // throw an exception for any unsupported value for BITs
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0005, " given bit value: " + csvString);
    }
  }

  // *********** DATE and TIME Column Type utils **********

  public static String toCSVDate(Object obj) {
    org.joda.time.LocalDate date = (org.joda.time.LocalDate) obj;
    return encloseWithQuotes(df.print(date));
  }

  public static String toCSVTime(Object obj, Column col) {
    if (((org.apache.sqoop.schema.type.Time) col).hasFraction()) {
      return encloseWithQuotes(tfWithFraction.print((org.joda.time.LocalTime) obj));
    } else {
      return encloseWithQuotes(tfWithNoFraction.print((org.joda.time.LocalTime) obj));
    }
  }

  public static Object toDate(String csvString, Column column) {
    return LocalDate.parse(removeQuotes(csvString));
  }

  public static Object toTime(String csvString, Column column) {
    return LocalTime.parse(removeQuotes(csvString));
  }

  // *********** DATE TIME Column Type utils **********

  public static String toCSVLocalDateTime(Object obj, Column col) {
    org.joda.time.LocalDateTime localDateTime = (org.joda.time.LocalDateTime) obj;
    org.apache.sqoop.schema.type.DateTime column = (org.apache.sqoop.schema.type.DateTime) col;
    if (column.hasFraction()) {
      return encloseWithQuotes(dtfWithFractionNoTimeZone.print(localDateTime));
    } else {
      return encloseWithQuotes(dtfWithNoFractionAndTimeZone.print(localDateTime));
    }
  }

  public static String toCSVDateTime(Object obj, Column col) {
    org.joda.time.DateTime dateTime = (org.joda.time.DateTime) obj;
    org.apache.sqoop.schema.type.DateTime column = (org.apache.sqoop.schema.type.DateTime) col;
    if (column.hasFraction() && column.hasTimezone()) {
      return encloseWithQuotes(dtfWithFractionAndTimeZone.print(dateTime));
    } else if (column.hasFraction() && !column.hasTimezone()) {
      return encloseWithQuotes(dtfWithFractionNoTimeZone.print(dateTime));
    } else if (column.hasTimezone()) {
      return encloseWithQuotes(dtfWithNoFractionWithTimeZone.print(dateTime));
    } else {
      return encloseWithQuotes(dtfWithNoFractionAndTimeZone.print(dateTime));
    }
  }

  public static Object toDateTime(String csvString, Column column) {
    Object returnValue;
    String dateTime = removeQuotes(csvString);
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

  public static Long toDateTimeInMillis(String csvString, Column column) {
    long returnValue;
    String dateTime = removeQuotes(csvString);
    org.apache.sqoop.schema.type.DateTime col = ((org.apache.sqoop.schema.type.DateTime) column);
    if (col.hasFraction() && col.hasTimezone()) {
      // After calling withOffsetParsed method, a string
      // '2004-06-09T10:20:30-08:00' will create a datetime with a zone of
      // -08:00 (a fixed zone, with no daylight savings rules)
      returnValue = dtfWithFractionAndTimeZone.withOffsetParsed().parseDateTime(dateTime).toDate().getTime();
    } else if (col.hasFraction() && !col.hasTimezone()) {
      // we use local date time explicitly to not include the timezone
      returnValue = dtfWithFractionNoTimeZone.parseLocalDateTime(dateTime).toDate().getTime();
    } else if (col.hasTimezone()) {
      returnValue = dtfWithNoFractionWithTimeZone.withOffsetParsed().parseDateTime(dateTime).toDate().getTime();
    } else {
      // we use local date time explicitly to not include the timezone
      returnValue = dtfWithNoFractionAndTimeZone.parseLocalDateTime(dateTime).toDate().getTime();
    }
    return returnValue;
  }

  // ************ MAP Column Type utils*********

  @SuppressWarnings("unchecked")
  public static String toCSVMap(Map<Object, Object> map, Column column) {
    JSONObject object = new JSONObject();
    object.putAll(map);
    return encloseWithQuotes(object.toJSONString());
  }

  public static Map<Object, Object> toMap(String csvString) {

    JSONObject object = null;
    try {
      object = (JSONObject) new JSONParser().parse(removeQuotes(csvString));
    } catch (org.json.simple.parser.ParseException e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0003, e);
    }
    if (object != null) {
      return toMap(object);
    }
    return null;
  }

  public static List<Object> toList(JSONArray array) {
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
  public static Map<Object, Object> toMap(JSONObject object) {
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
  public static String toCSVList(Object[] list, Column column) {
    List<Object> elementList = new ArrayList<Object>();
    for (int n = 0; n < list.length; n++) {
      Column listType = ((AbstractComplexListType) column).getListType();
      // 2 level nesting supported
      if (isColumnListType(listType)) {
        Object[] listElements = (Object[]) list[n];
        JSONArray subArray = new JSONArray();
        for (int i = 0; i < listElements.length; i++) {
          subArray.add(listElements[i]);
        }
        elementList.add(subArray);
      } else {
        elementList.add(list[n]);
      }
    }
    JSONArray array = new JSONArray();
    array.addAll(elementList);
    return encloseWithQuotes(array.toJSONString());
  }

  public static Object[] toList(String csvString) {

    JSONArray array = null;
    try {
      array = (JSONArray) new JSONParser().parse(removeQuotes(csvString));
    } catch (org.json.simple.parser.ParseException e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0003, e);
    }
    if (array != null) {
      return array.toArray();
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public static JSONArray toJSONArray(Object[] objectArray) {
    JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < objectArray.length; i++) {
      Object value = objectArray[i];
      if (value instanceof Object[]) {
        value = toJSONArray((Object[]) value);
      }
      jsonArray.add(value);
    }
    return jsonArray;
  }

  public static List<Object> toList(Object[] objectArray) {
    List<Object> objList = new ArrayList<Object>();
    for (int i = 0; i < objectArray.length; i++) {
      Object value = objectArray[i];
      if (value instanceof Object[]) {
        value = toList((Object[]) value);
      }
      objList.add(value);
    }
    return objList;
  }

  @SuppressWarnings("unchecked")
  public static Object[] toObjectArray(List<Object> list) {
    Object[] array = new Object[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object value = list.get(i);
      if (value instanceof List) {
        value = toObjectArray((List<Object>) value);
      }
      array[i] = value;
    }
    return array;
  }

  // ************ TEXT Column Type utils*********

  public static String toCSVString(String string) {
    StringBuilder sb1 = new StringBuilder();
    StringBuilder sb2 = new StringBuilder();

    // Escape the escape character
    for (int i = 0; i < string.length(); ++i) {
      char c = string.charAt(i);
      if (c == ESCAPE_CHARACTER) {
        sb1.append(ESCAPE_CHARACTER);
      }

      sb1.append(c);
    }

    // Encode characters
    for (char c : sb1.toString().toCharArray()) {
      if (ORIGINALS.containsKey(c)) {
        sb2.append(ORIGINALS.get(c));
      } else {
        sb2.append(c);
      }
    }

    return encloseWithQuotes(sb2.toString());
  }

  public static String toText(String string) {
    boolean escaped = false;
    StringBuilder sb = new StringBuilder();
    int i;

    // Remove the trailing and starting quotes.
    string = removeQuotes(string);

    // Decode
    for (i = 0; i < string.length(); ++i) {
      char c = string.charAt(i);

      if (escaped) {
        escaped = false;

        if (REPLACEMENTS.containsKey(c)) {
          c = REPLACEMENTS.get(c);
        }

        sb.append(c);
      } else {
        switch(c) {
          case ESCAPE_CHARACTER:
            escaped = true;
            break;

          default:
            sb.append(c);
            break;
        }
      }
    }

    return sb.toString();
  }

  // ************ BINARY Column type utils*********

  public static String toCSVByteArray(Object obj) {
    byte[] bytes = (byte[]) obj;
    try {
      return toCSVString(new String(bytes, BYTE_FIELD_CHARSET));
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

  public static String encloseWithQuotes(String string) {
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

  // ******* parse sqoop CSV ********

  /**
   * Encode to the sqoop prescribed CSV String for every element in the object
   * array
   *
   * @param objectArray
   */
  @SuppressWarnings("unchecked")
  public static String toCSV(Object[] objectArray, Schema schema) {
    Column[] columns = schema.getColumnsArray();

    StringBuilder csvString = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      if (objectArray[i] == null && !columns[i].isNullable()) {
        throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0005,
            columns[i].getName() + " does not support null values");
      }
      if (objectArray[i] == null) {
        csvString.append(NULL_VALUE);
      } else {
        switch (columns[i].getType()) {
          case ARRAY:
          case SET:
            csvString.append(toCSVList((Object[]) objectArray[i], (AbstractComplexListType) columns[i]));
            break;
          case MAP:
            csvString.append(toCSVMap((Map<Object, Object>) objectArray[i], columns[i]));
            break;
          case ENUM:
          case TEXT:
            csvString.append(toCSVString(objectArray[i].toString()));
            break;
          case BINARY:
          case UNKNOWN:
            csvString.append(toCSVByteArray((byte[]) objectArray[i]));
            break;
          case FIXED_POINT:
            csvString.append(toCSVFixedPoint(objectArray[i], columns[i]));
            break;
          case FLOATING_POINT:
            csvString.append(toCSVFloatingPoint(objectArray[i], columns[i]));
            break;
          case DECIMAL:
            csvString.append(toCSVDecimal(objectArray[i]));
            break;
          // stored in JSON as strings in the joda time format
          case DATE:
            csvString.append(toCSVDate(objectArray[i]));
            break;
          case TIME:
            csvString.append(toCSVTime(objectArray[i], columns[i]));
            break;
          case DATE_TIME:
            if (objectArray[i] instanceof org.joda.time.DateTime) {
              org.joda.time.DateTime dateTime = (org.joda.time.DateTime) objectArray[i];
              // check for fraction and time zone and then use the right formatter
              csvString.append(toCSVDateTime(dateTime, columns[i]));
            } else if (objectArray[i] instanceof org.joda.time.LocalDateTime) {
              org.joda.time.LocalDateTime localDateTime = (org.joda.time.LocalDateTime) objectArray[i];
              csvString.append(toCSVLocalDateTime(localDateTime, columns[i]));
            }
            break;
          case BIT:
            csvString.append(toCSVBit(objectArray[i]));
            break;
          default:
            throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
                "Column type from schema was not recognized for " + columns[i].getType());
        }
      }
      if (i < columns.length - 1) {
        csvString.append(CSV_SEPARATOR_CHARACTER);
      }

    }

    return csvString.toString();
  }

  /**
   * Custom CSV Text parser that honors quoting and escaped quotes.
   *
   * @return String[]
   */
  public static String[] parseCSVString(String csvText) {
    if (csvText == null) {
      return null;
    }

    boolean quoted = false;
    boolean escaped = false;

    List<String> parsedData = new LinkedList<String>();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < csvText.length(); ++i) {
      char c = csvText.charAt(i);
      switch (c) {
      case QUOTE_CHARACTER:
        builder.append(c);
        if (escaped) {
          escaped = false;
        } else {
          quoted = !quoted;
        }
        break;
      case ESCAPE_CHARACTER:
        builder.append(ESCAPE_CHARACTER);
        escaped = !escaped;
        break;
      case CSV_SEPARATOR_CHARACTER:
        if (quoted) {
          builder.append(c);
        } else {
          parsedData.add(builder.toString());
          builder = new StringBuilder();
        }
        break;
      default:
        if (escaped) {
          escaped = false;
        }
        builder.append(c);
        break;
      }
    }
    parsedData.add(builder.toString());

    return parsedData.toArray(new String[parsedData.size()]);
  }

  private static Object toObject(String csvString, Column column) {
    Object returnValue = null;

    switch (column.getType()) {
      case ENUM:
      case TEXT:
        returnValue = toText(csvString);
        break;
      case BINARY:
        // Unknown is treated as a binary type
      case UNKNOWN:
        returnValue = toByteArray(csvString);
        break;
      case FIXED_POINT:
        returnValue = toFixedPoint(csvString, column);
        break;
      case FLOATING_POINT:
        returnValue = toFloatingPoint(csvString, column);
        break;
      case DECIMAL:
        returnValue = toDecimal(csvString, column);
        break;
      case DATE:
        returnValue = toDate(csvString, column);
        break;
      case TIME:
        returnValue = toTime(csvString, column);
        break;
      case DATE_TIME:
        returnValue = toDateTime(csvString, column);
        break;
      case BIT:
        returnValue = toBit(csvString);
        break;
      case ARRAY:
      case SET:
        returnValue = toList(csvString);
        break;
      case MAP:
        returnValue = toMap(csvString);
        break;
      default:
        throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0004,
            "Column type from schema was not recognized for " + column.getType());
    }
    return returnValue;
  }

  /**
   * Parse CSV text data
   * @param csvText csv text to parse
   * @param schema schema to understand data
   * @return Object[]
   */
  public static Object[] fromCSV(String csvText, Schema schema) {
    String[] csvArray = parseCSVString(csvText);

    if (csvArray == null) {
      return null;
    }

    Column[] columns = schema.getColumnsArray();

    if (csvArray.length != columns.length) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
          "The data " + csvArray + " has the wrong number of fields.");
    }

    Object[] objectArray = new Object[csvArray.length];
    for (int i = 0; i < csvArray.length; i++) {
      if (csvArray[i].equals(NULL_VALUE) && !columns[i].isNullable()) {
        throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0005,
            columns[i].getName() + " does not support null values");
      }
      if (csvArray[i].equals(NULL_VALUE)) {
        objectArray[i] = null;
        continue;
      }
      objectArray[i] = toObject(csvArray[i], columns[i]);
    }

    return objectArray;
  }
}
