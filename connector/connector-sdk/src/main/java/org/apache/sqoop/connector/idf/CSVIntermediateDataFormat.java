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
  public void setCSVTextData(String csvText) {
    this.data = csvText;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] getObjectData() {
    if (schema == null || schema.isEmpty()) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0002);
    }

    // fieldStringArray represents the csv fields parsed into string array
    String[] csvStringArray = parseCSVString(this.data);

    if (csvStringArray == null) {
      return null;
    }

    if (csvStringArray.length != schema.getColumnsArray().length) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001, "The data " + getCSVTextData()
          + " has the wrong number of fields.");
    }

    Object[] objectArray = new Object[csvStringArray.length];
    Column[] columnArray = schema.getColumnsArray();
    for (int i = 0; i < csvStringArray.length; i++) {
      // check for NULL field and bail out immediately
      if (csvStringArray[i].equals(NULL_VALUE)) {
        objectArray[i] = null;
        continue;
      }
      objectArray[i] = toObject(csvStringArray[i], columnArray[i]);
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
   * {@inheritDoc}
   */
  @Override
  public void setObjectData(Object[] data) {
    Set<Integer> nullValueIndices = new HashSet<Integer>();
    Column[] columnArray = schema.getColumnsArray();
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
   *
   * {@inheritDoc}
   */
  @Override
  public void read(DataInput in) throws IOException {
    data = in.readUTF();
  }

  /**
   * Encode to the sqoop prescribed CSV String for every element in the object
   * array
   *
   * @param objectArray
   * @param columnArray
   * @param nullValueIndices
   */
  @SuppressWarnings("unchecked")
  private void encodeToCSVText(Object[] objectArray, Column[] columnArray, Set<Integer> nullValueIndices) {
    for (int i : bitTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVBit(objectArray[i]);
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
          objectArray[i] = encodeToCSVDateTime(dateTime, col);
        } else if (objectArray[i] instanceof org.joda.time.LocalDateTime) {
          org.joda.time.LocalDateTime localDateTime = (org.joda.time.LocalDateTime) objectArray[i];
          objectArray[i] = encodeToCSVLocalDateTime(localDateTime, col);
        }
      }
    }
    for (int i : dateTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVDate(objectArray[i]);
      }
    }
    for (int i : timeTypeColumnIndices) {
      Column col = columnArray[i];
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVTime(objectArray[i], col);
      }
    }
    for (int i : byteTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVByteArray((byte[]) objectArray[i]);
      }
    }
    for (int i : listTypeColumnIndices) {
      if (!nullValueIndices.contains(i)) {
        objectArray[i] = encodeToCSVList((Object[]) objectArray[i], (AbstractComplexListType) columnArray[i]);
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
  public Set<String> getJars() {

    Set<String> jars = super.getJars();
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