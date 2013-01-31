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
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;

/**
 * Imports records by transforming them to Avro records in an Avro data file.
 */
public class AvroImportMapper
    extends AutoProgressMapper<LongWritable, SqoopRecord,
    AvroWrapper<GenericRecord>, NullWritable> {

  private final AvroWrapper<GenericRecord> wrapper =
    new AvroWrapper<GenericRecord>();
  private Schema schema;
  private LargeObjectLoader lobLoader;
  private boolean bigDecimalFormatString;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    schema = AvroJob.getMapOutputSchema(conf);
    lobLoader = new LargeObjectLoader(conf,
        FileOutputFormat.getWorkOutputPath(context));
    bigDecimalFormatString = conf.getBoolean(
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
  }

  @Override
  protected void map(LongWritable key, SqoopRecord val, Context context)
      throws IOException, InterruptedException {

    try {
      // Loading of LOBs was delayed until we have a Context.
      val.loadLargeObjects(lobLoader);
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    }

    wrapper.datum(toGenericRecord(val));
    context.write(wrapper, NullWritable.get());
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
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
      if (bigDecimalFormatString) {
        return ((BigDecimal)o).toPlainString();
      } else {
        return o.toString();
      }
    } else if (o instanceof Date) {
      return ((Date) o).getTime();
    } else if (o instanceof Time) {
      return ((Time) o).getTime();
    } else if (o instanceof Timestamp) {
      return ((Timestamp) o).getTime();
    } else if (o instanceof BytesWritable) {
      BytesWritable bw = (BytesWritable) o;
      return ByteBuffer.wrap(bw.getBytes(), 0, bw.getLength());
    } else if (o instanceof BlobRef) {
      BlobRef br = (BlobRef) o;
      // If blob data is stored in an external .lob file, save the ref file
      // as Avro bytes. If materialized inline, save blob data as Avro bytes.
      byte[] bytes = br.isExternal() ? br.toString().getBytes() : br.getData();
      return ByteBuffer.wrap(bytes);
    } else if (o instanceof ClobRef) {
      throw new UnsupportedOperationException("ClobRef not suported");
    }
    // primitive types (Integer, etc) are left unchanged
    return o;
  }


}
