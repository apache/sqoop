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

package org.apache.sqoop.mapreduce.hcat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;

/**
 * A mapper that works on combined hcat splits.
 */
public class SqoopHCatExportMapper
    extends
  AutoProgressMapper<WritableComparable, HCatRecord,
  SqoopRecord, WritableComparable> {
  public static final Log LOG = LogFactory
    .getLog(SqoopHCatExportMapper.class.getName());
  private SqoopHCatExportHelper helper;

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();
    helper = new SqoopHCatExportHelper(conf);
  }

  @Override
  public void map(WritableComparable key, HCatRecord value,
    Context context)
    throws IOException, InterruptedException {
    context.write(helper.convertToSqoopRecord(value), NullWritable.get());
  }

}
