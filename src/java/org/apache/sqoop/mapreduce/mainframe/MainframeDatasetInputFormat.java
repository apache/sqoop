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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.util.MainframeFTPClientUtils;

/**
 * A InputFormat that retrieves a list of sequential dataset names in
 * a mainframe partitioned dataset. It then creates splits containing one or
 * more dataset names.
 */
public class MainframeDatasetInputFormat<T extends SqoopRecord>
    extends InputFormat<LongWritable, T>   {

  private static final Log LOG =
      LogFactory.getLog(MainframeDatasetInputFormat.class);

  @Override
  public RecordReader<LongWritable, T> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new MainframeDatasetFTPRecordReader<T>();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    Configuration conf = job.getConfiguration();
    String dsName
        = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
    LOG.info("Datasets to transfer from: " + dsName);
    List<String> datasets = retrieveDatasets(dsName, conf);
    if (datasets.isEmpty()) {
      throw new IOException ("No sequential datasets retrieved from " + dsName);
    } else {
      int count = datasets.size();
      int chunks = Math.min(count, ConfigurationHelper.getJobNumMaps(job));
      for (int i = 0; i < chunks; i++) {
        splits.add(new MainframeDatasetInputSplit());
      }

      int j = 0;
      while(j < count) {
        for (InputSplit sp : splits) {
          if (j == count) {
            break;
          }
          ((MainframeDatasetInputSplit)sp).addDataset(datasets.get(j));
          j++;
        }
      }
    }
    return splits;
  }

  protected List<String> retrieveDatasets(String dsName, Configuration conf)
      throws IOException {
    return MainframeFTPClientUtils.listSequentialDatasets(dsName, conf);
  }
}
