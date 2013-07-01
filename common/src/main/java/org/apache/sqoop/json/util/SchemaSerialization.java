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
package org.apache.sqoop.json.util;

import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.AbstractComplexType;
import org.apache.sqoop.schema.type.AbstractString;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.Array;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.Enum;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Map;
import org.apache.sqoop.schema.type.Set;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.schema.type.Time;
import org.apache.sqoop.schema.type.Type;
import org.apache.sqoop.schema.type.Unsupported;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 */
public class SchemaSerialization {

  private static final String NAME = "name";
  private static final String CREATION_DATE = "created";
  private static final String NOTE = "note";
  private static final String COLUMNS = "columns";
  private static final String TYPE = "type";
  private static final String NULLABLE = "nullable";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String SIZE = "size";
  private static final String FRACTION = "fraction";
  private static final String TIMEZONE = "timezone";
  private static final String PRECISION = "precision";
  private static final String SCALE = "scale";
  private static final String UNSIGNED = "unsigned";
  private static final String JDBC_TYPE = "jdbc-type";

  public static JSONObject extractSchema(Schema schema) {
    JSONObject object = new JSONObject();
    object.put(NAME, schema.getName());
    object.put(CREATION_DATE, schema.getCreationDate().getTime());
    if(schema.getNote() != null) {
      object.put(NOTE, schema.getNote());
    }

    JSONArray columnArray = new JSONArray();

    for(Column column : schema.getColumns()) {
      columnArray.add(extractColumn(column));
    }

    object.put(COLUMNS, columnArray);

    return object;
  }

  public static Schema restoreSchemna(JSONObject jsonObject) {
    String name = (String)jsonObject.get(NAME);
    String note = (String)jsonObject.get(NOTE);
    java.util.Date date = new java.util.Date((Long)jsonObject.get(CREATION_DATE));

    Schema schema = new Schema(name)
      .setNote(note)
      .setCreationDate(date);

    JSONArray columnsArray = (JSONArray)jsonObject.get(COLUMNS);
    for (Object obj : columnsArray) {
      schema.addColumn(restoreColumn((JSONObject)obj));
    }

    return schema;
  }

  private static JSONObject extractColumn(Column column) {
    JSONObject ret = new JSONObject();

    ret.put(NAME, column.getName());
    ret.put(NULLABLE, column.getNullable());
    ret.put(TYPE, column.getType().name());

    switch (column.getType()) {
      case MAP:
        ret.put(VALUE, extractColumn(((Map)column).getValue()));
      case ARRAY:
      case ENUM:
      case SET:
        ret.put(KEY, extractColumn(((AbstractComplexType) column).getKey()));
        break;
      case BINARY:
      case TEXT:
        ret.put(SIZE, ((AbstractString)column).getSize());
        break;
      case DATE_TIME:
        ret.put(FRACTION, ((DateTime)column).getFraction());
        ret.put(TIMEZONE, ((DateTime)column).getTimezone());
        break;
      case DECIMAL:
        ret.put(PRECISION, ((Decimal)column).getPrecision());
        ret.put(SCALE, ((Decimal)column).getScale());
        break;
      case FIXED_POINT:
        ret.put(SIZE, ((FixedPoint) column).getByteSize());
        ret.put(UNSIGNED, ((FixedPoint)column).getUnsigned());
        break;
      case FLOATING_POINT:
        ret.put(SIZE, ((FloatingPoint) column).getByteSize());
        break;
      case TIME:
        ret.put(FRACTION, ((Time)column).getFraction());
        break;
      case UNSUPPORTED:
        ret.put(JDBC_TYPE, ((Unsupported) column).getJdbcType());
        break;
      case DATE:
      case BIT:
        // Nothing to do extra
        break;
      default:
        // TODO(jarcec): Throw an exception of unsupported type?
    }

    return ret;
  }


  private static Column restoreColumn(JSONObject obj) {
    String name = (String) obj.get(NAME);

    Boolean nullable = (Boolean) obj.get(NULLABLE);
    Column key = null;
    if(obj.containsKey(KEY)) {
      key = restoreColumn((JSONObject) obj.get(KEY));
    }
    Column value = null;
    if(obj.containsKey(VALUE)) {
      value = restoreColumn((JSONObject) obj.get(VALUE));
    }
    Long size = (Long)obj.get(SIZE);
    Boolean fraction = (Boolean)obj.get(FRACTION);
    Boolean timezone = (Boolean)obj.get(TIMEZONE);
    Long precision = (Long)obj.get(PRECISION);
    Long scale = (Long)obj.get(SCALE);
    Boolean unsigned = (Boolean)obj.get(UNSIGNED);
    Long jdbcType = (Long)obj.get(JDBC_TYPE);

    Type type = Type.valueOf((String) obj.get(TYPE));
    Column output = null;
    switch (type) {
      case ARRAY:
        output = new Array(key);
        break;
      case BINARY:
        output = new Binary().setSize(size);
        break;
      case BIT:
        output = new Bit();
        break;
      case DATE:
        output = new Date();
        break;
      case DATE_TIME:
        output = new DateTime().setFraction(fraction).setTimezone(timezone);
        break;
      case DECIMAL:
        output = new Decimal().setPrecision(precision).setScale(scale);
        break;
      case ENUM:
        output = new Enum(key);
        break;
      case FIXED_POINT:
        output = new FixedPoint().setByteSize(size).setUnsigned(unsigned);
        break;
      case FLOATING_POINT:
        output = new FloatingPoint().setByteSize(size);
        break;
      case MAP:
        output = new Map(key, value);
        break;
      case SET:
        output = new Set(key);
        break;
      case TEXT:
        output = new Text().setSize(size);
        break;
      case TIME:
        output = new Time().setFraction(fraction);
        break;
      case UNSUPPORTED:
        output = new Unsupported().setJdbcType(jdbcType);
        break;
      default:
        // TODO(Jarcec): Throw an exception of unsupported type?
    }

    output.setName(name);
    output.setNullable(nullable);

    return output;
  }

  private SchemaSerialization() {
    // Serialization is prohibited
  }

}
