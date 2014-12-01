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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.AbstractComplexListType;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.ColumnType;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;


/**
 * A concrete implementation for the {@link #IntermediateDataFormat} that
 * represents each row of the data source as a comma separates list. Each
 * element in the CSV represents a specific column value encoded as string using the sqoop specified rules.
 * The methods allow serializing to this string and deserializing the string to its
 * corresponding java object based on the {@link #Schema} and its
 * {@link #Column} types.
 *
 */
public class CSVIntermediateDataFormat extends IntermediateDataFormat<String> {

  public static final Logger LOG = Logger.getLogger(CSVIntermediateDataFormat.class);

  public static final char SEPARATOR_CHARACTER = ',';
  public static final char ESCAPE_CHARACTER = '\\';
  public static final char QUOTE_CHARACTER = '\'';

  public static final String NULL_STRING = "NULL";

  private static final char[] originals = {
    0x5C,0x00,0x0A,0x0D,0x1A,0x22,0x27
  };


  private static final String[] replacements = {
    new String(new char[] { ESCAPE_CHARACTER, '\\'}),
    new String(new char[] { ESCAPE_CHARACTER, '0'}),
    new String(new char[] { ESCAPE_CHARACTER, 'n'}),
    new String(new char[] { ESCAPE_CHARACTER, 'r'}),
    new String(new char[] { ESCAPE_CHARACTER, 'Z'}),
    new String(new char[] { ESCAPE_CHARACTER, '\"'}),
    new String(new char[] { ESCAPE_CHARACTER, '\''})
  };

  // ISO-8859-1 is an 8-bit codec that is supported in every java
  // implementation.
  static final String BYTE_FIELD_CHARSET = "ISO-8859-1";

  private final List<Integer> stringTypeColumnIndices = new ArrayList<Integer>();
  private final List<Integer> byteTypeColumnIndices = new ArrayList<Integer>();
  private final List<Integer> listTypeColumnIndices = new ArrayList<Integer>();
  private final List<Integer> mapTypeColumnIndices = new ArrayList<Integer>();

  private Schema schema;

  public CSVIntermediateDataFormat() {
  }

  public CSVIntermediateDataFormat(Schema schema) {
    setSchema(schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getTextData() {
    return data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTextData(String text) {
    this.data = text;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSchema(Schema schema) {
    if (schema == null) {
      return;
    }
    this.schema = schema;
    List<Column> columns = schema.getColumns();
    int i = 0;
    for (Column col : columns) {
      if (col.getType() == ColumnType.TEXT) {
        stringTypeColumnIndices.add(i);
      } else if (col.getType() == ColumnType.BINARY) {
        byteTypeColumnIndices.add(i);
      } else if (isColumnListType(col)) {
        listTypeColumnIndices.add(i);
      } else if (col.getType() == ColumnType.MAP) {
        mapTypeColumnIndices.add(i);
      }
      i++;
    }
  }

  /**
   * Custom CSV parser that honors quoting and escaped quotes. All other
   * escaping is handled elsewhere.
   *
   * @return String[]
   */
  private String[] getFieldStringArray() {
    if (data == null) {
      return null;
    }

    boolean quoted = false;
    boolean escaped = false;

    List<String> parsedData = new LinkedList<String>();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < data.length(); ++i) {
      char c = data.charAt(i);
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
      case SEPARATOR_CHARACTER:
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

  /**
   * Converts the CSV String array into actual object array based on its
   * corresponding column type {@inheritDoc}
   */
  @Override
  public Object[] getObjectData() {
    if (schema == null || schema.isEmpty()) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0006);
    }

    // fieldStringArray represents the csv fields parsed into string array
    String[] fieldStringArray = getFieldStringArray();

    if (fieldStringArray == null) {
      return null;
    }

    if (fieldStringArray.length != schema.getColumns().size()) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0005,
          "The data " + getTextData() + " has the wrong number of fields.");
    }

    Object[] objectArray = new Object[fieldStringArray.length];
    Column[] columnArray = schema.getColumns().toArray(new Column[fieldStringArray.length]);
    for (int i = 0; i < fieldStringArray.length; i++) {
      // check for NULL field and bail out immediately
      if (fieldStringArray[i].equals("NULL")) {
        objectArray[i] = null;
        continue;
      }
      objectArray[i] = parseCSVStringArrayElement(fieldStringArray[i], columnArray[i]);
    }
    return objectArray;
  }

  private Object parseCSVStringArrayElement(String fieldString, Column column) {
    Object returnValue = null;

    switch (column.getType()) {
    case TEXT:
      returnValue = unescapeString(fieldString);
      break;
    case BINARY:
      // Unknown is treated as a binary type
    case UNKNOWN:
      returnValue = unescapeByteArray(fieldString);
      break;
    case FIXED_POINT:
      Long byteSize = ((FixedPoint) column).getByteSize();
      if (byteSize != null && byteSize <= Integer.SIZE) {
        returnValue = Integer.valueOf(fieldString);
      } else {
        returnValue = Long.valueOf(fieldString);
      }
      break;
    case FLOATING_POINT:
      byteSize = ((FloatingPoint) column).getByteSize();
      if (byteSize != null && byteSize <= Float.SIZE) {
        returnValue = Float.valueOf(fieldString);
      } else {
        returnValue = Double.valueOf(fieldString);
      }
      break;
    case DECIMAL:
      returnValue = new BigDecimal(fieldString);
      break;
    case DATE:
      returnValue = LocalDate.parse(fieldString);
      break;
    case DATE_TIME:
      // A datetime string with a space as date-time separator will not be
      // parsed expectedly. The expected separator is "T". See also:
      // https://github.com/JodaOrg/joda-time/issues/11
      String iso8601 = fieldString.replace(" ", "T");
      returnValue = LocalDateTime.parse(iso8601);
      break;
    case BIT:
      returnValue = Boolean.valueOf(fieldString.equals("1")
          || fieldString.toLowerCase().equals("true"));
      break;
    case ARRAY:
    case SET:
      returnValue = parseListElementFromJSON(fieldString);
      break;
    case MAP:
      returnValue = parseMapElementFromJSON(fieldString);
      break;
    default:
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0004,
          "Column type from schema was not recognized for " + column.getType());
    }
    return returnValue;
  }

  private Object[] parseListElementFromJSON(String fieldString) {

    JSONArray array = null;
    try {
      array = (JSONArray) new JSONParser().parse(removeQuotes(fieldString));
    } catch (org.json.simple.parser.ParseException e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0008, e);
    }
    if (array != null) {
      return array.toArray();
    }
    return null;
  }

  private Map<Object, Object> parseMapElementFromJSON(String fieldString) {

    JSONObject object = null;
    try {
      object = (JSONObject) new JSONParser().parse(removeQuotes(fieldString));
    } catch (org.json.simple.parser.ParseException e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0008, e);
    }
    if (object != null) {
      return toMap(object);
    }
    return null;
  }

  private List<Object> toList(JSONArray array) {
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
  private Map<Object, Object> toMap(JSONObject object) {
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

  /**
   * Appends the actual java objects into CSV string {@inheritDoc}
   */
  @Override
  public void setObjectData(Object[] data) {
    Column[] columnArray = schema.getColumns().toArray(new Column[data.length]);
    encodeCSVStringElements(data, columnArray);
    this.data = StringUtils.join(data, SEPARATOR_CHARACTER);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.data);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void read(DataInput in) throws IOException {
    data = in.readUTF();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || !(other instanceof CSVIntermediateDataFormat)) {
      return false;
    }
    return data.equals(((CSVIntermediateDataFormat) other).data);
  }

  public int compareTo(IntermediateDataFormat<?> o) {
    if (this == o) {
      return 0;
    }
    if (this.equals(o)) {
      return 0;
    }
    if (!(o instanceof CSVIntermediateDataFormat)) {
      throw new IllegalStateException("Expected Data to be instance of "
          + "CSVIntermediateFormat, but was an instance of " + o.getClass().getName());
    }
    return data.compareTo(o.getTextData());
  }

  /**
   * Sanitize every element of the CSV string based on the column type
   *
   * @param stringArray
   */
  @SuppressWarnings("unchecked")
  private void encodeCSVStringElements(Object[] stringArray, Column[] columnArray) {
    for (int i : stringTypeColumnIndices) {
      stringArray[i] = escapeString((String) stringArray[i]);
    }
    for (int i : byteTypeColumnIndices) {
      stringArray[i] = escapeByteArrays((byte[]) stringArray[i]);
    }
    for (int i : listTypeColumnIndices) {
      stringArray[i] = encodeList((Object[]) stringArray[i], columnArray[i]);
    }
    for (int i : mapTypeColumnIndices) {
      stringArray[i] = encodeMap((Map<Object, Object>) stringArray[i], columnArray[i]);
    }
  }

  @SuppressWarnings("unchecked")
  private String encodeMap(Map<Object, Object> map, Column column) {
    JSONObject object = new JSONObject();
    object.putAll(map);
    return encloseWithQuote(object.toJSONString());
  }

  @SuppressWarnings("unchecked")
  private String encodeList(Object[] list, Column column) {
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

  private boolean isColumnListType(Column listType) {
    return listType.getType().equals(ColumnType.ARRAY) || listType.getType().equals(ColumnType.SET);
  }

  private String escapeByteArrays(byte[] bytes) {
    try {
      return escapeString(new String(bytes, BYTE_FIELD_CHARSET));
    } catch (UnsupportedEncodingException e) {
      // We should never hit this case.
      // This character set should be distributed with Java.
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
          "The character set " + BYTE_FIELD_CHARSET + " is not available.");
    }
  }

  private String getRegExp(char orig) {
    return getRegExp(String.valueOf(orig));
  }

  private String getRegExp(String orig) {
    return orig.replaceAll("\\\\", Matcher.quoteReplacement("\\\\"));
  }

  private String escapeString(String orig) {
    if (orig == null) {
      return NULL_STRING;
    }

    int j = 0;
    String replacement = orig;
    try {
      for (j = 0; j < replacements.length; j++) {
        replacement = replacement.replaceAll(getRegExp(originals[j]),
            Matcher.quoteReplacement(replacements[j]));
      }
    } catch (Exception e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0002, orig
          + "  " + replacement + "  " + String.valueOf(j) + "  " + e.getMessage());
    }
    return encloseWithQuote(replacement);
  }

  private String encloseWithQuote(String string) {
    StringBuilder builder = new StringBuilder();
    builder.append(QUOTE_CHARACTER).append(string).append(QUOTE_CHARACTER);
    return builder.toString();
  }

  private String unescapeString(String orig) {
    // Remove the trailing and starting quotes.
    orig = removeQuotes(orig);
    int j = 0;
    try {
      for (j = 0; j < replacements.length; j++) {
        orig = orig.replaceAll(getRegExp(replacements[j]),
            Matcher.quoteReplacement(String.valueOf(originals[j])));
      }
    } catch (Exception e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0003, orig
          + "  " + String.valueOf(j) + e.getMessage());
    }

    return orig;
  }

  private String removeQuotes(String string) {
    return string.substring(1, string.length() - 1);
  }

  private byte[] unescapeByteArray(String orig) {
    // Always encoded in BYTE_FIELD_CHARSET.
    try {
      return unescapeString(orig).getBytes(BYTE_FIELD_CHARSET);
    } catch (UnsupportedEncodingException e) {
      // Should never hit this case.
      // This character set should be distributed with Java.
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
          "The character set " + BYTE_FIELD_CHARSET + " is not available.");
    }
  }

  public String toString() {
    return data;
  }
}
