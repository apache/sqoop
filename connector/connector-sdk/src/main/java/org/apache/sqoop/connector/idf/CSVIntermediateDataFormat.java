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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.IntermediateDataFormatError;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.AbstractComplexListType;
import org.apache.sqoop.schema.type.Column;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * A concrete implementation for the {@link #IntermediateDataFormat} that
 * represents each row of the data source as a comma separates list. Each
 * element in the CSV represents a specific column value encoded as string using
 * the sqoop specified rules. The methods allow serializing to this string and
 * deserializing the string to its corresponding java object based on the
 * {@link #Schema} and its {@link #Column} types.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class CSVIntermediateDataFormat extends IntermediateDataFormat<String> {

  public static final Logger LOG = Logger.getLogger(CSVIntermediateDataFormat.class);

  // need this default constructor for reflection magic used in execution engine
  public CSVIntermediateDataFormat() {
  }

  public CSVIntermediateDataFormat(Schema schema) {
    super.setSchema(schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCSVTextData() {
    return super.getData();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCSVTextData(String csvText) {
    super.setData(csvText);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] getObjectData() {
    super.validateSchema(schema);
    String[] csvStringArray = parseCSVString(this.data);

    if (csvStringArray == null) {
      return null;
    }
    Column[] columns = schema.getColumnsArray();

    if (csvStringArray.length != columns.length) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
          "The data " + getCSVTextData() + " has the wrong number of fields.");
    }

    Object[] objectArray = new Object[csvStringArray.length];
    for (int i = 0; i < csvStringArray.length; i++) {
      if (csvStringArray[i].equals(NULL_VALUE) && !columns[i].isNullable()) {
        throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0005,
            columns[i].getName() + " does not support null values");
      }
      if (csvStringArray[i].equals(NULL_VALUE)) {
        objectArray[i] = null;
        continue;
      }
      objectArray[i] = toObject(csvStringArray[i], columns[i]);
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
    super.validateSchema(schema);
    // convert object array to csv text
    this.data = toCSV(data);

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
   */
  @SuppressWarnings("unchecked")
  private String toCSV(Object[] objectArray) {

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
   * {@inheritDoc}
   */
  @Override
  public Set<String> getJars() {
    return super.getJars();
  }
}
