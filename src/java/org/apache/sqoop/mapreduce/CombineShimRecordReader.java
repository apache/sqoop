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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * RecordReader that CombineFileRecordReader can instantiate, which itself
 * translates a CombineFileSplit into a FileSplit.
 */
public class CombineShimRecordReader
   extends RecordReader<LongWritable, Object> {

  public static final Log LOG =
     LogFactory.getLog(CombineShimRecordReader.class.getName());

  private CombineFileSplit split;
  private TaskAttemptContext context;
  private int index;
  private RecordReader<LongWritable, Object> rr;

  /**
   * Constructor invoked by CombineFileRecordReader that identifies part of a
   * CombineFileSplit to use.
   */
  public CombineShimRecordReader(CombineFileSplit split,
      TaskAttemptContext context, Integer index)
      throws IOException, InterruptedException {
    this.index = index;
    this.split = (CombineFileSplit) split;
    this.context = context;

    createChildReader();
  }

  @Override
  public void initialize(InputSplit curSplit, TaskAttemptContext curContext)
      throws IOException, InterruptedException {
    this.split = (CombineFileSplit) curSplit;
    this.context = curContext;

    if (null == rr) {
      createChildReader();
    }

    FileSplit fileSplit = new FileSplit(this.split.getPath(index),
        this.split.getOffset(index), this.split.getLength(index),
        this.split.getLocations());
    this.rr.initialize(fileSplit, this.context);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return rr.getProgress();
  }

  @Override
  public void close() throws IOException {
    if (null != rr) {
      rr.close();
      rr = null;
    }
  }

  @Override
  public LongWritable getCurrentKey()
      throws IOException, InterruptedException {
    return rr.getCurrentKey();
  }

  @Override
  public Object getCurrentValue()
      throws IOException, InterruptedException {
    return rr.getCurrentValue();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return rr.nextKeyValue();
  }

  /**
   * Actually instantiate the user's chosen RecordReader implementation.
   */
  @SuppressWarnings("unchecked")
  private void createChildReader() throws IOException, InterruptedException {
    LOG.debug("ChildSplit operates on: " + split.getPath(index));

    Configuration conf = context.getConfiguration();

    // Determine the file format we're reading.
    Class rrClass;
    if (ExportJobBase.isSequenceFiles(conf, split.getPath(index))) {
      rrClass = SequenceFileRecordReader.class;
    } else {
      rrClass = LineRecordReader.class;
    }

    // Create the appropriate record reader.
    this.rr = (RecordReader<LongWritable, Object>)
        ReflectionUtils.newInstance(rrClass, conf);
  }
}
