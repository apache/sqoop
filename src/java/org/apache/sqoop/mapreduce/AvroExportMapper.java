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

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.cloudera.sqoop.orm.ClassWriter;

/**
 * Exports records from an Avro data file.
 */
public class AvroExportMapper
    extends AutoProgressMapper<AvroWrapper<GenericRecord>, NullWritable,
              SqoopRecord, NullWritable> {

  private static final String TIMESTAMP_TYPE = "java.sql.Timestamp";

  private static final String TIME_TYPE = "java.sql.Time";

  private static final String DATE_TYPE = "java.sql.Date";

  private static final String BIG_DECIMAL_TYPE = "java.math.BigDecimal";

  public static final String AVRO_COLUMN_TYPES_MAP =
      "sqoop.avro.column.types.map";

  private MapWritable columnTypes;
  private SqoopRecord recordImpl;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {

    super.setup(context);

    Configuration conf = context.getConfiguration();

    // Instantiate a copy of the user's class to hold and parse the record.
    String recordClassName = conf.get(
        ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY);
    if (null == recordClassName) {
      throw new IOException("Export table class name ("
          + ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY
          + ") is not set!");
    }

    try {
      Class cls = Class.forName(recordClassName, true,
          Thread.currentThread().getContextClassLoader());
      recordImpl = (SqoopRecord) ReflectionUtils.newInstance(cls, conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }

    if (null == recordImpl) {
      throw new IOException("Could not instantiate object of type "
          + recordClassName);
    }

    columnTypes = DefaultStringifier.load(conf, AVRO_COLUMN_TYPES_MAP,
        MapWritable.class);
  }

  @Override
  protected void map(AvroWrapper<GenericRecord> key, NullWritable value,
      Context context) throws IOException, InterruptedException {
    context.write(toSqoopRecord(key.datum()), NullWritable.get());
  }

  private SqoopRecord toSqoopRecord(GenericRecord record) throws IOException {
    Schema avroSchema = record.getSchema();
    for (Map.Entry<Writable, Writable> e : columnTypes.entrySet()) {
      String columnName = e.getKey().toString();
      String columnType = e.getValue().toString();
      String cleanedCol = ClassWriter.toIdentifier(columnName);
      Field field = getField(avroSchema, cleanedCol, record);
      if (field == null) {
        throw new IOException("Cannot find field " + cleanedCol
          + " in Avro schema " + avroSchema);
      } else {
        Object avroObject = record.get(field.name());
        Object fieldVal = fromAvro(avroObject, field.schema(), columnType);
        recordImpl.setField(cleanedCol, fieldVal);
      }
    }
    return recordImpl;
  }

  private Field getField(Schema avroSchema, String fieldName,
      GenericRecord record) {
    for (Field field : avroSchema.getFields()) {
      if (field.name().equalsIgnoreCase(fieldName)) {
        return field;
      }
    }
    return null;
  }

  private Object fromAvro(Object avroObject, Schema fieldSchema,
      String columnType) {
    // map from Avro type to Sqoop's Java representation of the SQL type
    // see SqlManager#toJavaType

    if (avroObject == null) {
      return null;
    }

    switch (fieldSchema.getType()) {
      case NULL:
        return null;
      case BOOLEAN:
      case INT:
      case FLOAT:
      case DOUBLE:
        return avroObject;
      case LONG:
        if (columnType.equals(DATE_TYPE)) {
          return new Date((Long) avroObject);
        } else if (columnType.equals(TIME_TYPE)) {
          return new Time((Long) avroObject);
        } else if (columnType.equals(TIMESTAMP_TYPE)) {
          return new Timestamp((Long) avroObject);
        }
        return avroObject;
      case BYTES:
        ByteBuffer bb = (ByteBuffer) avroObject;
        BytesWritable bw = new BytesWritable();
        bw.set(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
        return bw;
      case STRING:
        if (columnType.equals(BIG_DECIMAL_TYPE)) {
          return new BigDecimal(avroObject.toString());
        } else if (columnType.equals(DATE_TYPE)) {
          return Date.valueOf(avroObject.toString());
        } else if (columnType.equals(TIME_TYPE)) {
          return Time.valueOf(avroObject.toString());
        } else if (columnType.equals(TIMESTAMP_TYPE)) {
          return Timestamp.valueOf(avroObject.toString());
        }
        return avroObject.toString();
      case ENUM:
        return ((GenericEnumSymbol) avroObject).toString();
      case UNION:
        List<Schema> types = fieldSchema.getTypes();
        if (types.size() != 2) {
          throw new IllegalArgumentException("Only support union with null");
        }
        Schema s1 = types.get(0);
        Schema s2 = types.get(1);
        if (s1.getType() == Schema.Type.NULL) {
          return fromAvro(avroObject, s2, columnType);
        } else if (s2.getType() == Schema.Type.NULL) {
          return fromAvro(avroObject, s1, columnType);
        } else {
          throw new IllegalArgumentException("Only support union with null");
        }
      case FIXED:
        return new BytesWritable(((GenericFixed) avroObject).bytes());
      case RECORD:
      case ARRAY:
      case MAP:
      default:
        throw new IllegalArgumentException("Cannot convert Avro type "
            + fieldSchema.getType());
    }
  }

}
