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

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.IntermediateDataFormatError;
import org.apache.sqoop.schema.type.AbstractComplexListType;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SqoopAvroUtils {

  private static final Logger LOG = Logger.getLogger(SqoopAvroUtils.class);

  public static final String COLUMN_TYPE = "columnType";
  public static final String SQOOP_SCHEMA_NAMESPACE = "org.apache.sqoop";

  /**
   * Creates an Avro schema from a Sqoop schema.
   */
  public static Schema createAvroSchema(org.apache.sqoop.schema.Schema sqoopSchema) {
    // avro schema names cannot start with quotes, lets just remove them
    String name = createAvroName(sqoopSchema.getName());
    String doc = sqoopSchema.getNote();
    String namespace = SQOOP_SCHEMA_NAMESPACE;
    Schema schema = Schema.createRecord(name, doc, namespace, false);

    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (Column column : sqoopSchema.getColumnsArray()) {
      Schema.Field field = new Schema.Field(createAvroName(column.getName()), createAvroFieldSchema(column), null, null);
      field.addProp(COLUMN_TYPE, column.getType().toString());
      fields.add(field);
    }
    schema.setFields(fields);
    return schema;
  }

  // From the avro docs:
  // The name portion of a fullname, record field names, and enum symbols must:
  // start with [A-Za-z_]
  // subsequently contain only [A-Za-z0-9_]
  public static String createAvroName(String name) {
    String avroName = name.replaceFirst("^[0-9]", "").replaceAll("[^a-zA-Z0-9_]", "");
    LOG.debug("Replacing name: " + name + " with Avro name: " + avroName);
    return avroName;
  }

  public static Schema createAvroFieldSchema(Column column) {
    Schema schema = toAvroFieldType(column);
    if (!column.isNullable()) {
      return schema;
    } else {
      List<Schema> union = new ArrayList<Schema>();
      union.add(schema);
      union.add(Schema.create(Schema.Type.NULL));
      return Schema.createUnion(union);
    }
  }

  public static Schema toAvroFieldType(Column column) throws IllegalArgumentException {
    switch (column.getType()) {
    case ARRAY:
    case SET:
      assert column instanceof AbstractComplexListType;
      AbstractComplexListType listColumn = (AbstractComplexListType) column;
      return Schema.createArray(toAvroFieldType(listColumn.getListType()));
    case UNKNOWN:
    case BINARY:
      return Schema.create(Schema.Type.BYTES);
    case BIT:
      return Schema.create(Schema.Type.BOOLEAN);
    case DATE:
    case DATE_TIME:
    case TIME:
      // avro 1.8 will have date type
      // https://issues.apache.org/jira/browse/AVRO-739
      return Schema.create(Schema.Type.LONG);
    case DECIMAL:
      // TODO: is string ok, used it since kite code seems to use it
      return Schema.create(Schema.Type.STRING);
    case ENUM:
      return createEnumSchema(column);
    case FIXED_POINT:
      if (SqoopIDFUtils.isInteger(column)) {
        return Schema.create(Schema.Type.INT);
      } else {
        return Schema.create(Schema.Type.LONG);
      }
    case FLOATING_POINT:
      assert column instanceof FloatingPoint;
      Long byteSize = ((FloatingPoint) column).getByteSize();
      if (byteSize != null && byteSize <= (Float.SIZE/Byte.SIZE)) {
        return Schema.create(Schema.Type.FLOAT);
      } else {
        return Schema.create(Schema.Type.DOUBLE);
      }
    case MAP:
      assert column instanceof org.apache.sqoop.schema.type.Map;
      org.apache.sqoop.schema.type.Map mapColumn = (org.apache.sqoop.schema.type.Map) column;
      return Schema.createArray(toAvroFieldType(mapColumn.getValue()));
    case TEXT:
      return Schema.create(Schema.Type.STRING);
    default:
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0004, column.getType().name());
    }
  }

  public static Schema createEnumSchema(Column column) {
    assert column instanceof org.apache.sqoop.schema.type.Enum;
    Set<String> options = ((org.apache.sqoop.schema.type.Enum) column).getOptions();
    List<String> listOptions = new ArrayList<String>(options);
    return Schema.createEnum(createAvroName(column.getName()), null, SQOOP_SCHEMA_NAMESPACE, listOptions);
  }

  public static byte[] getBytesFromByteBuffer(Object obj) {
    ByteBuffer buffer = (ByteBuffer) obj;
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

}
