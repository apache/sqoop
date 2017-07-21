
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
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.sqoop.avro.AvroUtil;

import com.cloudera.sqoop.lib.SqoopRecord;


public class MergeParquetReducer extends Reducer<Text, MergeRecord,GenericRecord,NullWritable> {

  private Schema schema = null;
  private boolean bigDecimalFormatString = true;
  private Map<String, Pair<String, String>> sqoopRecordFields = new HashMap<String, Pair<String, String>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      schema = new Schema.Parser().parse(context.getConfiguration().get("parquetjob.avro.schema"));
      bigDecimalFormatString = context.getConfiguration().getBoolean(
          ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT, ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
    }

    @Override
    public void reduce(Text key, Iterable<MergeRecord> vals, Context context)
        throws IOException, InterruptedException {
      SqoopRecord bestRecord = null;
      try {
        for (MergeRecord mergeRecord : vals) {
          if (null == bestRecord && !mergeRecord.isNewRecord()) {
            // Use an old record if we don't have a new record.
            bestRecord = (SqoopRecord) mergeRecord.getSqoopRecord().clone();
          } else if (mergeRecord.isNewRecord()) {
            bestRecord = (SqoopRecord) mergeRecord.getSqoopRecord().clone();
          }
        }
      } catch (CloneNotSupportedException cnse) {
        throw new IOException(cnse);
      }

      if (null != bestRecord) {
        GenericRecord outKey = AvroUtil.toGenericRecord(bestRecord.getFieldMap(), schema,
            bigDecimalFormatString);
        context.write(outKey, null);
      }
    }
}