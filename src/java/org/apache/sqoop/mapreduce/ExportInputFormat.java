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
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat that generates a user-defined number of splits to inject data
 * into the database.
 */
public class ExportInputFormat
   extends CombineFileInputFormat<LongWritable, Object> {

  public static final Log LOG =
     LogFactory.getLog(ExportInputFormat.class.getName());

  public static final int DEFAULT_NUM_MAP_TASKS = 4;

  public ExportInputFormat() {
  }

  /**
   * @return the number of bytes across all files in the job.
   */
  private long getJobSize(JobContext job) throws IOException {
    List<FileStatus> stats = listStatus(job);
    long count = 0;
    for (FileStatus stat : stats) {
      count += stat.getLen();
    }

    return count;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    // Set the max split size based on the number of map tasks we want.
    long numTasks = getNumMapTasks(job);
    long numFileBytes = getJobSize(job);
    long maxSplitSize = numFileBytes / numTasks;

    setMaxSplitSize(maxSplitSize);

    LOG.debug("Target numMapTasks=" + numTasks);
    LOG.debug("Total input bytes=" + numFileBytes);
    LOG.debug("maxSplitSize=" + maxSplitSize);

    List<InputSplit> splits =  super.getSplits(job);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated splits:");
      for (InputSplit split : splits) {
        LOG.debug("  " + split);
      }
    }
    return splits;
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordReader createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {

    CombineFileSplit combineSplit = (CombineFileSplit) split;

    // Use CombineFileRecordReader since this can handle CombineFileSplits
    // and instantiate another RecordReader in a loop; do this with the
    // CombineShimRecordReader.
    RecordReader rr = new CombineFileRecordReader(combineSplit, context,
        CombineShimRecordReader.class);

    return rr;
  }

  /**
   * Allows the user to control the number of map tasks used for this
   * export job.
   */
  public static void setNumMapTasks(JobContext job, int numTasks) {
    job.getConfiguration().setInt(ExportJobBase.EXPORT_MAP_TASKS_KEY, numTasks);
  }

  /**
   * @return the number of map tasks to use in this export job.
   */
  public static int getNumMapTasks(JobContext job) {
    return job.getConfiguration().getInt(ExportJobBase.EXPORT_MAP_TASKS_KEY,
        DEFAULT_NUM_MAP_TASKS);
  }

}
