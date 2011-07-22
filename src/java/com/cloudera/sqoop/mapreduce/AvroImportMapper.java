/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.mapreduce;

import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.SqoopRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Imports records by transforming them to Avro records in an Avro data file.
 */
public class AvroImportMapper
    extends AutoProgressMapper<LongWritable, SqoopRecord,
              AvroWrapper<GenericRecord>, NullWritable> {

  private final AvroWrapper<GenericRecord> wrapper =
    new AvroWrapper<GenericRecord>();
  private Schema schema;

  @Override
  protected void setup(Context context) {
    schema = AvroJob.getMapOutputSchema(context.getConfiguration());
  }

  @Override
  protected void map(LongWritable key, SqoopRecord val, Context context)
      throws IOException, InterruptedException {
    wrapper.datum(toGenericRecord(val));
    context.write(wrapper, NullWritable.get());
  }


  private GenericRecord toGenericRecord(SqoopRecord val) {
    Map<String, Object> fieldMap = val.getFieldMap();
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      record.put(entry.getKey(), toAvro(entry.getValue()));
    }
    return record;
  }

  /**
   * Convert the Avro representation of a Java type (that has already been
   * converted from the SQL equivalent).
   * @param o
   * @return
   */
  private Object toAvro(Object o) {
    if (o instanceof BigDecimal) {
      return o.toString();
    } else if (o instanceof Date) {
      return ((Date) o).getTime();
    } else if (o instanceof Time) {
      return ((Time) o).getTime();
    } else if (o instanceof Timestamp) {
      return ((Timestamp) o).getTime();
    } else if (o instanceof ClobRef) {
      throw new UnsupportedOperationException("ClobRef not suported");
    } else if (o instanceof BlobRef) {
      throw new UnsupportedOperationException("BlobRef not suported");
    }
    // primitive types (Integer, etc) are left unchanged
    return o;
  }


}
