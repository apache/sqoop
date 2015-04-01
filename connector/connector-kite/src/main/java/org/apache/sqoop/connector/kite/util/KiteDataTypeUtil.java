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
package org.apache.sqoop.connector.kite.util;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.ColumnType;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * The helper class provides methods to convert Sqoop data types to Kite
 * supported data types.
 */
public class KiteDataTypeUtil {

  public static final String SQOOP_TYPE = "SqoopType";
  public static final String DEFAULT_SQOOP_SCHEMA_NAMESPACE = "sqoop";
  private static final ImmutableMap<FileFormat, Format> TO_FORMAT_LOOKUP =
      ImmutableMap.<FileFormat, Format>builder()
          .put(FileFormat.CSV, Formats.CSV)
          .put(FileFormat.AVRO, Formats.AVRO)
          .put(FileFormat.PARQUET, Formats.PARQUET)
          .build();

  /**
   * Creates an Avro schema from a Sqoop schema.
   */
  public static Schema createAvroSchema(
      org.apache.sqoop.schema.Schema sqoopSchema) {
    String name = sqoopSchema.getName();
    String doc = sqoopSchema.getNote();
    String namespace = DEFAULT_SQOOP_SCHEMA_NAMESPACE;
    Schema schema = Schema.createRecord(toAvroName(name), doc, namespace, false);

    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (Column column : sqoopSchema.getColumnsArray()) {
      Schema.Field field = new Schema.Field(toAvroName(column.getName()),
          createAvroFieldSchema(column), null, null);
      field.addProp(SQOOP_TYPE, column.getType().toString());
      fields.add(field);
    }
    schema.setFields(fields);
    return schema;
  }

  private static Schema createAvroFieldSchema(Column column) {
    Schema.Type type = toAvroType(column);
    if (!column.isNullable()) {
      return Schema.create(type);
    } else {
      List<Schema> union = new ArrayList<Schema>();
      union.add(Schema.create(type));
      union.add(Schema.create(Schema.Type.NULL));
      return Schema.createUnion(union);
    }
  }

  /**
   * Converts arbitrary string to valid Avro name.
   *
   * This method does not guarantee that different two strings
   * ends up as different two strings after conversion. It's
   * up to the caller to ensure uniqueness if/when needed.
   *
   * Valid Avro names:
   * * Starts with [A-Za-z_]
   * * Subsequently only [A-Za-z0-9_]
   *
   * http://avro.apache.org/docs/1.7.7/spec.html#Names   *
   *
   * @param name
   * @return
   */
  static String toAvroName(String name) {
    if(name == null || name.isEmpty()) {
      return name;
    }

    // If we're not starting with [A-Za-z_], prepend '_'
    if(name.charAt(0) != '_' && !Character.isLetter(name.charAt(0)) ) {
      name = "_" + name;
    }

    // Otherwise replace all invalid characters with '_'
    return name.replaceAll("[^0-9A-Za-z_]", "_");
  }

  private static Schema.Type toAvroType(Column column) throws IllegalArgumentException {
    switch (column.getType()) {
      case ARRAY:
        return Schema.Type.ARRAY;
      case BINARY:
        return Schema.Type.BYTES;
      case BIT:
        return Schema.Type.BOOLEAN;
      case DATE:
      case DATE_TIME:
      case TIME:
        // TODO: SQOOP-1616
        return Schema.Type.LONG;
      case DECIMAL:
        // TODO: SQOOP-1616
        return Schema.Type.STRING;
      case ENUM:
      case SET:
        return Schema.Type.ENUM;
      case FIXED_POINT: {
        FixedPoint fp = (FixedPoint)column;
        if(fp.getByteSize() <= 4L) {
          return Schema.Type.INT;
        } else if(fp.getByteSize() <= 8L) {
          return Schema.Type.LONG;
        } else {
          throw new IllegalArgumentException("Unsupported size of FixedType column " + fp.getByteSize());
        }
      }
      case FLOATING_POINT:
        FloatingPoint fp = (FloatingPoint)column;
        if(fp.getByteSize() <= 4L) {
          return Schema.Type.FLOAT;
        } else if(fp.getByteSize() <= 8L) {
          return Schema.Type.DOUBLE;
        } else {
          throw new IllegalArgumentException("Unsupported size of FloatingPoint column " + fp.getByteSize());
        }
      case MAP:
        return Schema.Type.MAP;
      case TEXT:
        return Schema.Type.STRING;
      case UNKNOWN:
        return Schema.Type.NULL;
      default:
        throw new IllegalArgumentException("Unsupported Sqoop Data Type " + column.getType());
    }
  }

  /**
   * Creates a GenericRecord instance from a Sqoop record.
   */
  public static GenericRecord createGenericRecord(Object[] array,
      Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    List<Schema.Field> fields = schema.getFields();

    assert array.length == fields.size();
    for (int i = 0; i < array.length; i++) {
      String key = fields.get(i).name();
      Object value = toAvro(array[i]);
      record.put(key, value);
    }
    return record;
  }

  private static Object toAvro(Object o) {
    if (o instanceof BigDecimal) {
      return ((BigDecimal) o).toPlainString();
    } else if (o instanceof LocalDate) {
      return ((LocalDate) o).toDate().getTime();
    } else if (o instanceof LocalDateTime) {
      return ((LocalDateTime) o).toDate().getTime();
    }
    return o;
  }

  /**
   * Converts Sqoop (user input) FileFormat to Kite supported file format.
   */
  public static Format toFormat(FileFormat format)
      throws IllegalArgumentException {
    if (!TO_FORMAT_LOOKUP.containsKey(format)) {
      throw new IllegalArgumentException(
          "Unsupported File Output Format " + format);
    }
    return TO_FORMAT_LOOKUP.get(format);
  }

}