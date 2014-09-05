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

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.cloudera.sqoop.orm.ClassWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.avro.AvroUtil;

import java.io.IOException;
import java.util.Map;

/**
 * Exports records (type GenericRecord) from a data source.
 */
public class GenericRecordExportMapper<K, V>
    extends AutoProgressMapper<K, V, SqoopRecord, NullWritable> {

  public static final String AVRO_COLUMN_TYPES_MAP = "sqoop.avro.column.types.map";

  protected MapWritable columnTypes;

  private SqoopRecord recordImpl;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
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

  protected SqoopRecord toSqoopRecord(GenericRecord record) throws IOException {
    Schema avroSchema = record.getSchema();
    for (Map.Entry<Writable, Writable> e : columnTypes.entrySet()) {
      String columnName = e.getKey().toString();
      String columnType = e.getValue().toString();
      String cleanedCol = ClassWriter.toIdentifier(columnName);
      Schema.Field field = getFieldIgnoreCase(avroSchema, cleanedCol);
      if (null == field) {
        throw new IOException("Cannot find field " + cleanedCol
            + " in Avro schema " + avroSchema);
      }

      Object avroObject = record.get(field.name());
      Object fieldVal = AvroUtil.fromAvro(avroObject, field.schema(), columnType);
      recordImpl.setField(cleanedCol, fieldVal);
    }
    return recordImpl;
  }

  private static Schema.Field getFieldIgnoreCase(Schema avroSchema,
      String fieldName) {
    for (Schema.Field field : avroSchema.getFields()) {
      if (field.name().equalsIgnoreCase(fieldName)) {
        return field;
      }
    }
    return null;
  }

}
