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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.Pair;

import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.lib.SqoopRecord;


public class MergeParquetMapper
    extends  MergeGenericRecordExportMapper<GenericRecord, GenericRecord> {

  private Map<String, Pair<String, String>> sqoopRecordFields = new HashMap<String, Pair<String, String>>();
  private SqoopRecord sqoopRecordImpl;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    final String userClassName = conf.get(MergeJob.MERGE_SQOOP_RECORD_KEY);
    try {
      final Class<? extends Object> clazz = Class.forName(userClassName, true,
          Thread.currentThread().getContextClassLoader());
      sqoopRecordImpl = (SqoopRecord) ReflectionUtils.newInstance(clazz, conf);
      for (final Field field : clazz.getDeclaredFields()) {
        final String fieldName = field.getName();
        final String fieldTypeName = field.getType().getName();
        sqoopRecordFields.put(fieldName.toLowerCase(), new Pair<String, String>(fieldName,
            fieldTypeName));
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Cannot find the user record class with class name"
          + userClassName, e);
    }
  }

  @Override
  protected void map(GenericRecord key, GenericRecord val, Context context)
      throws IOException, InterruptedException {
    processRecord(toSqoopRecord(val), context);
  }

  private SqoopRecord toSqoopRecord(GenericRecord genericRecord) throws IOException {
    Schema avroSchema = genericRecord.getSchema();
    for (Schema.Field field : avroSchema.getFields()) {
      Pair<String, String> sqoopRecordField = sqoopRecordFields.get(field.name().toLowerCase());
      if (null == sqoopRecordField) {
        throw new IOException("Cannot find field '" + field.name() + "' in fields of user class"
            + sqoopRecordImpl.getClass().getName() + ". Fields are: "
            + Arrays.deepToString(sqoopRecordFields.values().toArray()));
      }
      Object avroObject = genericRecord.get(field.name());
      Object fieldVal = AvroUtil.fromAvro(avroObject, field.schema(), sqoopRecordField.value());
      sqoopRecordImpl.setField(sqoopRecordField.key(), fieldVal);
    }
    return sqoopRecordImpl;
  }

}