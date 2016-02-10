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

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Exports records from an Avro data file.
 */
public class AvroExportMapper
    extends GenericRecordExportMapper<AvroWrapper<GenericRecord>, NullWritable> {

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    // Add decimal support
    ReflectData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
  }

  @Override
  protected void map(AvroWrapper<GenericRecord> key, NullWritable value,
      Context context) throws IOException, InterruptedException {
    context.write(toSqoopRecord(key.datum()), NullWritable.get());
  }

}
