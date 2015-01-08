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

import org.apache.commons.codec.binary.Base64;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.utils.ClassUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * IDF representing the intermediate format in JSON
 */
public class JSONIntermediateDataFormat extends IntermediateDataFormat<JSONObject> {

  // We need schema at all times
  public JSONIntermediateDataFormat(Schema schema) {
    setSchema(schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCSVTextData(String text) {
    // convert the CSV text to JSON
    this.data = toJSON(text);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCSVTextData() {
    // convert JSON to sqoop CSV
    return toCSV(data);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setObjectData(Object[] data) {
    // convert the object Array to JSON
    this.data = toJSON(data);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] getObjectData() {
    // convert JSON to object array
    return toObject(data);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.data.toJSONString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void read(DataInput in) throws IOException {
    try {
      data = (JSONObject) new JSONParser().parse(in.readUTF());
    } catch (ParseException e) {
      throw new SqoopException(JSONIntermediateDataFormatError.JSON_INTERMEDIATE_DATA_FORMAT_0002, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<String> getJars() {

    Set<String> jars = super.getJars();
    jars.add(ClassUtils.jarForClass(JSONObject.class));
    return jars;
  }

  @SuppressWarnings("unchecked")
  private JSONObject toJSON(String csv) {

    String[] csvStringArray = parseCSVString(csv);

    if (csvStringArray == null) {
      return null;
    }

    if (csvStringArray.length != schema.getColumnsArray().length) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001, "The data " + csv
          + " has the wrong number of fields.");
    }
    JSONObject object = new JSONObject();
    Column[] columnArray = schema.getColumnsArray();
    for (int i = 0; i < csvStringArray.length; i++) {
      // check for NULL field and bail out immediately
      if (csvStringArray[i].equals(NULL_VALUE)) {
        object.put(columnArray[i].getName(), null);
        continue;
      }
      object.put(columnArray[i].getName(), toJSON(csvStringArray[i], columnArray[i]));

    }
    return object;
  }

  private Object toJSON(String csvString, Column column) {
    Object returnValue = null;

    switch (column.getType()) {
    case ARRAY:
    case SET:
      try {
        returnValue = (JSONArray) new JSONParser().parse(removeQuotes(csvString));
      } catch (ParseException e) {
        throw new SqoopException(JSONIntermediateDataFormatError.JSON_INTERMEDIATE_DATA_FORMAT_0002, e);
      }
      break;
    case MAP:
      try {
        returnValue = (JSONObject) new JSONParser().parse(removeQuotes(csvString));
      } catch (ParseException e) {
        throw new SqoopException(JSONIntermediateDataFormatError.JSON_INTERMEDIATE_DATA_FORMAT_0003, e);
      }
      break;
    case ENUM:
    case TEXT:
      returnValue = toText(csvString);
      break;
    case BINARY:
    case UNKNOWN:
      returnValue = Base64.encodeBase64String(toByteArray(csvString));
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
    case TIME:
    case DATE_TIME:
      // store as string expected to be in the JODA time format in CSV
      // stored in JSON as joda time format
      returnValue = removeQuotes(csvString);
      break;
    // true/false and TRUE/ FALSE are the only accepted values for JSON Bit
    // will be stored as true/false in JSON
    case BIT:
      returnValue = Boolean.valueOf(removeQuotes(csvString));
      break;
    default:
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0004,
          "Column type from schema was not recognized for " + column.getType());
    }
    return returnValue;
  }

  @SuppressWarnings("unchecked")
  private JSONObject toJSON(Object[] data) {

    if (data == null) {
      return null;
    }

    if (data.length != schema.getColumnsArray().length) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001, "The data " + data.toString()
          + " has the wrong number of fields.");
    }
    JSONObject object = new JSONObject();
    Column[] cols = schema.getColumnsArray();
    for (int i = 0; i < data.length; i++) {
      switch (cols[i].getType()) {
      case ARRAY:
      case SET:
        // store as JSON array
        Object[] objArray = (Object[]) data[i];
        JSONArray jsonArray = toJSONArray(objArray);
        object.put(cols[i].getName(), jsonArray);
        break;
      case MAP:
        // store as JSON object
        Map<Object, Object> map = (Map<Object, Object>) data[i];
        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(map);
        object.put(cols[i].getName(), jsonObject);
        break;
      case ENUM:
      case TEXT:
        object.put(cols[i].getName(), data[i]);
        break;
      case BINARY:
      case UNKNOWN:
        object.put(cols[i].getName(), Base64.encodeBase64String((byte[]) data[i]));
        break;
      case FIXED_POINT:
      case FLOATING_POINT:
      case DECIMAL:
        // store a object
        object.put(cols[i].getName(), data[i]);
        break;
      // stored in JSON as the same format as csv strings in the joda time
      // format
      case DATE_TIME:
        object.put(cols[i].getName(), removeQuotes(encodeToCSVDateTime(data[i], cols[i])));
        break;
      case TIME:
        object.put(cols[i].getName(), removeQuotes(encodeToCSVTime(data[i], cols[i])));
        break;
      case DATE:
        object.put(cols[i].getName(), removeQuotes(encodeToCSVDate(data[i])));
        break;
      case BIT:
        object.put(cols[i].getName(), Boolean.valueOf(encodeToCSVBit(data[i])));
        break;
      default:
        throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
            "Column type from schema was not recognized for " + cols[i].getType());
      }
    }

    return object;
  }

  private String toCSV(JSONObject json) {
    Column[] cols = this.schema.getColumnsArray();

    StringBuilder csvString = new StringBuilder();
    for (int i = 0; i < cols.length; i++) {

      // or we can to json.entrySet();
      Object obj = json.get(cols[i].getName());
      if (obj == null) {
        throw new SqoopException(JSONIntermediateDataFormatError.JSON_INTERMEDIATE_DATA_FORMAT_0003, " for " + cols[i].getName());
      }

      switch (cols[i].getType()) {
      case ARRAY:
      case SET:
        // stored as JSON array
        JSONArray array = (JSONArray) obj;
        csvString.append(encloseWithQuote(array.toJSONString()));
        break;
      case MAP:
        // stored as JSON object
        csvString.append(encloseWithQuote((((JSONObject) obj).toJSONString())));
        break;
      case ENUM:
      case TEXT:
        csvString.append(encodeToCSVString(obj.toString()));
        break;
      case BINARY:
      case UNKNOWN:
        csvString.append(encodeToCSVByteArray(Base64.decodeBase64(obj.toString())));
        break;
      case FIXED_POINT:
        csvString.append(encodeToCSVFixedPoint(obj, cols[i]));
        break;
      case FLOATING_POINT:
        csvString.append(encodeToCSVFloatingPoint(obj, cols[i]));
        break;
      case DECIMAL:
        csvString.append(encodeToCSVDecimal(obj));
        break;
      // stored in JSON as strings in the joda time format
      case DATE:
      case TIME:
      case DATE_TIME:
        csvString.append(encloseWithQuote(obj.toString()));
        break;
      // 0/1 will be stored as they are in JSON, even though valid values in
      // JSON
      // are true/false
      case BIT:
        csvString.append(encodeToCSVBit(obj));
        break;
      default:
        throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
            "Column type from schema was not recognized for " + cols[i].getType());
      }
      if (i < cols.length - 1) {
        csvString.append(CSV_SEPARATOR_CHARACTER);
      }

    }

    return csvString.toString();
  }

  @SuppressWarnings("unchecked")
  private Object[] toObject(JSONObject data) {

    if (data == null) {
      return null;
    }
    Column[] cols = schema.getColumnsArray();
    Object[] object = new Object[cols.length];

    Set<String> keys = data.keySet();
    int i = 0;
    for (String key : keys) {

      Integer nameIndex = schema.getColumnNameIndex(key);
      Object obj = data.get(key);
      Column column = cols[nameIndex];
      switch (column.getType()) {
      case ARRAY:
      case SET:
        object[nameIndex] = toList((JSONArray) obj).toArray();
        break;
      case MAP:
        object[nameIndex] = toMap((JSONObject) obj);
        break;
      case ENUM:
      case TEXT:
        object[nameIndex] = toText(obj.toString());
        break;
      case BINARY:
      case UNKNOWN:
        // JSON spec is to store byte array as base64 encoded
        object[nameIndex] = Base64.decodeBase64(obj.toString());
        break;
      case FIXED_POINT:
        object[nameIndex] = toFixedPoint(obj.toString(), column);
        break;
      case FLOATING_POINT:
        object[nameIndex] = toFloatingPoint(obj.toString(), column);
        break;
      case DECIMAL:
        object[nameIndex] = toDecimal(obj.toString(), column);
        break;
      case DATE:
        object[nameIndex] = toDate(obj.toString(), column);
        break;
      case TIME:
        object[nameIndex] = toTime(obj.toString(), column);
        break;
      case DATE_TIME:
        object[nameIndex] = toDateTime(obj.toString(), column);
        break;
      case BIT:
        object[nameIndex] = toBit(obj.toString());
        break;
      default:
        throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001,
            "Column type from schema was not recognized for " + cols[i].getType());
      }

    }
    return object;
  }

}
