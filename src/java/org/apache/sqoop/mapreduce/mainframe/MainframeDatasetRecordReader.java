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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.sqoop.mapreduce.DBWritable;

import org.apache.sqoop.mapreduce.db.DBConfiguration;

/**
 * A RecordReader that reads records from a mainframe dataset.
 * Emits LongWritables containing the record number as key and DBWritables as
 * value.
 */
public abstract class MainframeDatasetRecordReader <T extends DBWritable>
    extends RecordReader<LongWritable, T> {
  private Class<T> inputClass;
  private Configuration conf;
  private MainframeDatasetInputSplit split;
  private LongWritable key;
  private T datasetRecord;
  private long numberRecordRead;
  private int datasetProcessed;

  private static final Log LOG = LogFactory.getLog(
      MainframeDatasetRecordReader.class.getName());

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    split = (MainframeDatasetInputSplit)inputSplit;
    conf = taskAttemptContext.getConfiguration();
    inputClass = (Class<T>) (conf.getClass(
                DBConfiguration.INPUT_CLASS_PROPERTY, null));
    key = null;
    datasetRecord = null;
    numberRecordRead = 0;
    datasetProcessed = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (key == null) {
      key = new LongWritable();
    }
    if (datasetRecord == null) {
      datasetRecord = ReflectionUtils.newInstance(inputClass, conf);
    }
    if (getNextRecord(datasetRecord)) {
      numberRecordRead++;
      key.set(numberRecordRead);
      return true;
    }
    return false;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public T getCurrentValue() throws IOException, InterruptedException {
    return datasetRecord;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return datasetProcessed / (float)split.getLength();
  }

  protected String getNextDataset() {
    String datasetName = split.getNextDataset();
    if (datasetName != null) {
      datasetProcessed++;
      LOG.info("Starting transfer of " + datasetName);
    }
    return datasetName;
  }

  protected Configuration getConfiguration() {
    return conf;
  }

  protected abstract boolean getNextRecord (T datasetRecord) throws IOException;

}
