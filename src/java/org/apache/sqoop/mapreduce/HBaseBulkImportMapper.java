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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.hbase.PutTransformer;
import org.apache.sqoop.hbase.ToStringPutTransformer;

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import static org.apache.sqoop.hbase.HBasePutProcessor.*;

/**
 * Imports records by writing them to HBase via the DelegatingOutputFormat
 * and the HBasePutProcessor.
 */
public class HBaseBulkImportMapper
    extends AutoProgressMapper
    <LongWritable, SqoopRecord, ImmutableBytesWritable, Put> {

  private LargeObjectLoader lobLoader;
  //An object that can transform a map of fieldName->object
  // into a Put command.
  private PutTransformer putTransformer;
  private Configuration conf;
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    this.conf = context.getConfiguration();
    this.lobLoader = new LargeObjectLoader(this.conf, new Path( this.conf.get("sqoop.hbase.lob.extern.dir", "/tmp/sqoop-hbase-" + context.getTaskAttemptID())));

    // Get the implementation of PutTransformer to use.
    // By default, we call toString() on every non-null field.
    Class<? extends PutTransformer> xformerClass =
        (Class<? extends PutTransformer>)
        this.conf.getClass(TRANSFORMER_CLASS_KEY, ToStringPutTransformer.class);
    this.putTransformer = (PutTransformer)
        ReflectionUtils.newInstance(xformerClass, this.conf);
    if (null == putTransformer) {
      throw new RuntimeException("Could not instantiate PutTransformer.");
    }
    this.putTransformer.setColumnFamily(conf.get(COL_FAMILY_KEY, null));
    this.putTransformer.setRowKeyColumn(conf.get(ROW_KEY_COLUMN_KEY, null));
  }
  @Override
  public void map(LongWritable key, SqoopRecord val, Context context)
      throws IOException, InterruptedException {
    try {
      // Loading of LOBs was delayed until we have a Context.
      val.loadLargeObjects(lobLoader);
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    }
    Map<String, Object> fields = val.getFieldMap();

    List<Put> putList = putTransformer.getPutCommand(fields);
    for(Put put: putList){
      context.write(new ImmutableBytesWritable(put.getRow()), put);
    }
  }
  @Override
  protected void cleanup(Context context) throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
  }
}

