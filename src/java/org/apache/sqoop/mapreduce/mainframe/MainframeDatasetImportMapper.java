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

package org.apache.sqoop.mapreduce.mainframe;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;

/**
 * Mapper that writes mainframe dataset records in Text format to multiple files
 * based on the key, which is the index of the datasets in the input split.
 */
public class MainframeDatasetImportMapper
    extends AutoProgressMapper<LongWritable, SqoopRecord, Text, NullWritable> {

  private static final Log LOG = LogFactory.getLog(
      MainframeDatasetImportMapper.class.getName());

  private MainframeDatasetInputSplit inputSplit;
  private MultipleOutputs<Text, NullWritable> mos;
  private long numberOfRecords;
  private Text outkey;

  public void map(LongWritable key,  SqoopRecord val, Context context)
      throws IOException, InterruptedException {
    String dataset = inputSplit.getCurrentDataset();
    outkey.set(val.toString());
    numberOfRecords++;
    mos.write(outkey, NullWritable.get(), dataset);
  }

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    inputSplit = (MainframeDatasetInputSplit)context.getInputSplit();
    mos = new MultipleOutputs<Text, NullWritable>(context);
    numberOfRecords = 0;
    outkey = new Text();
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    super.cleanup(context);
    mos.close();
    context.getCounter(
        ConfigurationConstants.COUNTER_GROUP_MAPRED_TASK_COUNTERS,
        ConfigurationConstants.COUNTER_MAP_OUTPUT_RECORDS)
        .increment(numberOfRecords);
  }
}
