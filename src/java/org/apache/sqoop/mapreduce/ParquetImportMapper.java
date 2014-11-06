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

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.sqoop.avro.AvroUtil;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Imports records by writing them to a Parquet File.
 */
public class ParquetImportMapper
    extends AutoProgressMapper<LongWritable, SqoopRecord,
        GenericRecord, NullWritable> {

  private Schema schema = null;
  private boolean bigDecimalFormatString = true;
  private LargeObjectLoader lobLoader = null;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    schema = ParquetJob.getAvroSchema(conf);
    bigDecimalFormatString = conf.getBoolean(
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
    lobLoader = new LargeObjectLoader(conf, new Path(conf.get("sqoop.kite.lob.extern.dir", "/tmp/sqoop-parquet-" + context.getTaskAttemptID())));
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

    GenericRecord outKey = AvroUtil.toGenericRecord(val.getFieldMap(), schema,
        bigDecimalFormatString);
    context.write(outKey, null);
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
  }

}
