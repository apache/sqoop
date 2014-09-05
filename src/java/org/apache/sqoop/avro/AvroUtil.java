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
package org.apache.sqoop.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.BytesWritable;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/**
 * The service class provides methods for creating and converting Avro objects.
 */
public final class AvroUtil {

  /**
   * Convert a Sqoop's Java representation to Avro representation.
   */
  public static Object toAvro(Object o, boolean bigDecimalFormatString) {
    if (o instanceof BigDecimal) {
      if (bigDecimalFormatString) {
        // Returns a string representation of this without an exponent field.
        return ((BigDecimal) o).toPlainString();
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
      throw new UnsupportedOperationException("ClobRef not supported");
    }
    // primitive types (Integer, etc) are left unchanged
    return o;
  }

  /**
   * Manipulate a GenericRecord instance.
   */
  public static GenericRecord toGenericRecord(Map<String, Object> fieldMap,
      Schema schema, boolean bigDecimalFormatString) {
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      Object avroObject = toAvro(entry.getValue(), bigDecimalFormatString);
      record.put(entry.getKey(), avroObject);
    }
    return record;
  }

}
