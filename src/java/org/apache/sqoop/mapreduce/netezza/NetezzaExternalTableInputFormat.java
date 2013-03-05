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

package org.apache.sqoop.mapreduce.netezza;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.config.ConfigurationHelper;

/**
 * InputFormat designed to take data-driven splits and use them in the netezza
 * external table import invocation running in the mapper.
 *
 * The key emitted by this mapper is the data slice id to for the Netezza
 * external table query.
 */
public class NetezzaExternalTableInputFormat extends
    InputFormat<Integer, NullWritable> {

  public static final Log LOG = LogFactory
      .getLog(NetezzaExternalTableInputFormat.class.getName());

  /**
   * A RecordReader that just takes the WHERE conditions from the DBInputSplit
   * and relates them to the mapper as a single input record.
   */
  public static class NetezzaExternalTableRecordReader extends
      RecordReader<Integer, NullWritable> {

    private boolean delivered;
    private InputSplit split;

    public NetezzaExternalTableRecordReader(InputSplit split) {
      initialize(split, null);
    }

    @Override
    public boolean nextKeyValue() {
      boolean hasNext = !delivered;
      delivered = true;
      return hasNext;
    }

    @Override
    public Integer getCurrentKey() {
      return ((NetezzaExternalTableInputSplit) this.split).getDataSliceId();
    }

    @Override
    public NullWritable getCurrentValue() {
      return NullWritable.get();
    }

    @Override
    public void close() {
    }

    @Override
    public float getProgress() {
      return delivered ? 1.0f : 0.0f;
    }

    @Override
    public void initialize(InputSplit s, TaskAttemptContext context) {
      this.split = s;
      this.delivered = false;
    }
  }

  public RecordReader<Integer, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new NetezzaExternalTableRecordReader(split);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    int targetNumTasks = ConfigurationHelper.getJobNumMaps(context);
    List<InputSplit> splits = new ArrayList<InputSplit>(targetNumTasks);
    for (int i = 0; i < targetNumTasks; ++i) {
      splits.add(new NetezzaExternalTableInputSplit(i));
    }
    return splits;
  }

}
