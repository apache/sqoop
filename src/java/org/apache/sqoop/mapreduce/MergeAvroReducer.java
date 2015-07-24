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
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.sqoop.avro.AvroUtil;
import com.cloudera.sqoop.lib.SqoopRecord;

public class MergeAvroReducer extends MergeReducerBase<AvroWrapper<GenericRecord>, NullWritable> {
  private AvroWrapper<GenericRecord> wrapper;
  private Schema schema;
  private boolean bigDecimalFormatString;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    wrapper = new AvroWrapper<GenericRecord>();
    schema = AvroJob.getOutputSchema(context.getConfiguration());
    bigDecimalFormatString = context.getConfiguration().getBoolean(
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT, ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
  }

  @Override
  protected void writeRecord(SqoopRecord record, Context context)
      throws IOException, InterruptedException {
    GenericRecord outKey = AvroUtil.toGenericRecord(record.getFieldMap(), schema,
        bigDecimalFormatString);
    wrapper.datum(outKey);
    context.write(wrapper, NullWritable.get());
  }
}
