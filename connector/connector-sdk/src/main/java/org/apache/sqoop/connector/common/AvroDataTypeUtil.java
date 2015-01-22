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
import org.apache.avro.generic.GenericRecord;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.schema.type.Unknown;

import java.util.List;

/**
 * The helper class provides methods to convert Sqoop data types to Avro
 * supported data types.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AvroDataTypeUtil {

  public static org.apache.sqoop.schema.Schema createSqoopSchema(
      Schema avroSchema) {
    org.apache.sqoop.schema.Schema schema =
        new org.apache.sqoop.schema.Schema(avroSchema.getName());
    schema.setNote(avroSchema.getDoc());
    for (Schema.Field field : avroSchema.getFields()) {
      Column column = avroTypeToSchemaType(field);
      schema.addColumn(column);
    }
    return schema;
  }

  private static Column avroTypeToSchemaType(Schema.Field field) {
    Schema.Type schemaType = field.schema().getType();
    if (schemaType == Schema.Type.UNION) {
      List<Schema> unionSchema = field.schema().getTypes();
      if (unionSchema.size() == 2) {
        Schema.Type first = unionSchema.get(0).getType();
        Schema.Type second = unionSchema.get(1).getType();
        if ((first == Schema.Type.NULL && second != Schema.Type.NULL) ||
            (first != Schema.Type.NULL && second == Schema.Type.NULL)) {
          return avroPrimitiveTypeToSchemaType(field.name(),
              first != Schema.Type.NULL ? first : second);
        }
      }
      // This is an unsupported complex data type
      return new Unknown(field.name());
    }

    return avroPrimitiveTypeToSchemaType(field.name(), schemaType);
  }

  private static Column avroPrimitiveTypeToSchemaType(String name,
      Schema.Type type) {
    assert type != Schema.Type.UNION;
    switch (type) {
      case INT:
        return new FixedPoint(name, 4L, true);
      case LONG:
        return new FixedPoint(name, 8L, true);
      case STRING:
        return new Text(name);
      case DOUBLE:
        return new FloatingPoint(name, 8L);
      case BOOLEAN:
        return new Bit(name);
      case BYTES:
        return new Binary(name);
      default:
        return new Unknown(name);
    }
  }

  public static Object[] extractGenericRecord(GenericRecord data) {
    List<Schema.Field> fields = data.getSchema().getFields();
    Object[] record = new Object[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      record[i] = data.get(i);
    }
    return record;
  }

}