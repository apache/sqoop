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

import static org.apache.sqoop.connector.common.SqoopIDFUtils.*;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.AbstractComplexListType;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.ColumnType;
import org.apache.sqoop.utils.ClassUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private final Set<Integer> stringTypeColumnIndices = new HashSet<Integer>();
  private final Set<Integer> bitTypeColumnIndices = new HashSet<Integer>();
  private final Set<Integer> byteTypeColumnIndices = new HashSet<Integer>();
  private final Set<Integer> listTypeColumnIndices = new HashSet<Integer>();
  private final Set<Integer> mapTypeColumnIndices = new HashSet<Integer>();
  private final Set<Integer> dateTimeTypeColumnIndices = new HashSet<Integer>();
  private final Set<Integer> dateTypeColumnIndices = new HashSet<Integer>();
  private final Set<Integer> timeColumnIndices = new HashSet<Integer>();


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
  public String getCSVTextData() {
    return data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCSVTextData(String text) {
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
      if (isColumnStringType(col)) {
        stringTypeColumnIndices.add(i);
      } else if (col.getType() == ColumnType.BIT) {
        bitTypeColumnIndices.add(i);
      } else if (col.getType() == ColumnType.DATE) {
        dateTypeColumnIndices.add(i);
      } else if (col.getType() == ColumnType.TIME) {
        timeColumnIndices.add(i);
      } else if (col.getType() == ColumnType.DATE_TIME) {
        dateTimeTypeColumnIndices.add(i);
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
   * Custom CSV Text parser that honors quoting and escaped quotes.
   *
   * @return String[]
   */
  private String[] parseCSVString() {
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

  /**
   * Converts the CSV String array into actual object array based on its
   * corresponding column type {@inheritDoc}
   */
  @Override
  public Object[] getObjectData() {
    if (schema == null || schema.isEmpty()) {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0006);
    }

    // fieldStringArray represents the csv fields parsed into string array
    String[] fieldStringArray = parseCSVString();

    if (fieldStringArray == null) {
      return null;
    }

    if (fieldStringArray.length != schema.getColumns().size()) {
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0005,
          "The data " + getCSVTextData() + " has the wrong number of fields.");
    }

    Object[] objectArray = new Object[fieldStringArray.length];
    Column[] columnArray = schema.getColumns().toArray(new Column[fieldStringArray.length]);
    for (int i = 0; i < fieldStringArray.length; i++) {
      // check for NULL field and bail out immediately
      if (fieldStringArray[i].equals(NULL_VALUE)) {
        objectArray[i] = null;
        continue;
      }
      objectArray[i] = toObject(fieldStringArray[i], columnArray[i]);
    }
    return objectArray;
  }

  private Object toObject(String csvString, Column column) {
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
      returnValue = toBit(csvString, returnValue);
      break;
    case ARRAY:
    case SET:
      returnValue = toList(csvString);
      break;
    case MAP:
      returnValue = toMap(csvString);
      break;
    default:
      throw new SqoopException(CSVIntermediateDataFormatError.CSV_INTERMEDIATE_DATA_FORMAT_0004,
          "Column type from schema was not recognized for " + column.getType());
    }
    return returnValue;
  }

 

  /**
   * Appends the actual java objects into CSV string {@inheritDoc}
   */
  @Override
  public void setObjectData(Object[] data) {
   Set<Integer> nullValueIndices = new HashSet<Integer>();
    Column[] columnArray = schema.getColumns().toArray(new Column[data.length]);
    // check for null
    for (int i = 0; i < data.length; i++) {
      if (data[i] == null) {
        nullValueIndices.add(i);
        data[i] = NULL_VALUE;
      }
    }
    // ignore the null values while encoding the object array into csv string
    encodeToCSVText(data, columnArray, nullValueIndices);
    this.data = StringUtils.join(data, CSV_SEPARATOR_CHARACTER);
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

 /**
  * Encode to the sqoop prescribed CSV String for every element in the objet array
  * @param objectArray
  * @param columnArray
  * @param nullValueIndices
  */
  @SuppressWarnings("unchecked")
  private void encodeToCSVText(Object[] objectArray, Column[] columnArray, Set<Integer> nullValueIndices) {
    for (int i : bitTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        encodeToCSVBit(objectArray, i);
      }
    }
    for (int i : stringTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVString((String) objectArray[i]);
      }
    }
    for (int i : dateTimeTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        Column col = columnArray[i];
        if (objectArray[i] instanceof org.joda.time.DateTime) {
          org.joda.time.DateTime dateTime = (org.joda.time.DateTime) objectArray[i];
          // check for fraction and time zone and then use the right formatter
          encodeToCSVDateTime(objectArray, i, col, dateTime);
        } else if (objectArray[i] instanceof org.joda.time.LocalDateTime) {
          org.joda.time.LocalDateTime localDateTime = (org.joda.time.LocalDateTime) objectArray[i];
          encodeToCSVLocalDateTime(objectArray, i, col, localDateTime);
        }
      }
    }
    for (int i : dateTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        encodeToCSVDate(objectArray, i);
      }
    }
    for (int i : timeColumnIndices) {
      Column col = columnArray[i];
      if (!nullValueIndices.contains(i)) {
        encodeToCSVTime(objectArray, i, col);
      }
    }
    for (int i : byteTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVByteArray((byte[]) objectArray[i]);
      }
    }
    for (int i : listTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVList((Object[]) objectArray[i], (AbstractComplexListType)columnArray[i]);
      }
    }
    for (int i : mapTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVMap((Map<Object, Object>) objectArray[i], columnArray[i]);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> getJars() {

    List<String> jars = super.getJars();
    // Add JODA classes for IDF date/time handling
    jars.add(ClassUtils.jarForClass(LocalDate.class));
    jars.add(ClassUtils.jarForClass(LocalDateTime.class));
    jars.add(ClassUtils.jarForClass(DateTime.class));
    jars.add(ClassUtils.jarForClass(LocalTime.class));
    // Add JSON parsing jar
    jars.add(ClassUtils.jarForClass(JSONValue.class));
    return jars;
  }

}
