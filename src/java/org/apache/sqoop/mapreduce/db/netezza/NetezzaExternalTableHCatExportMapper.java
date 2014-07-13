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
package org.apache.sqoop.mapreduce.db.netezza;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatExportHelper;

/**
 * Netezza export mapper using external tables for HCat integration.
 */
public class NetezzaExternalTableHCatExportMapper extends
  NetezzaExternalTableExportMapper<LongWritable, HCatRecord> {
  private SqoopHCatExportHelper helper;
  public static final Log LOG = LogFactory
    .getLog(NetezzaExternalTableHCatExportMapper.class.getName());

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    helper = new SqoopHCatExportHelper(conf);
    // Force escaped by
    conf.setInt(DelimiterSet.OUTPUT_ESCAPED_BY_KEY, '\'');

  }

  @Override
  public void map(LongWritable key, HCatRecord hcr, Context context)
    throws IOException, InterruptedException {
    SqoopRecord sqr = helper.convertToSqoopRecord(hcr);
    writeSqoopRecord(sqr);
    context.progress();
  }
}
