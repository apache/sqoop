/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.sqoop.shims;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mrunit.mapreduce.mock.MockReporter;

/**
 * Hadoop Shim for Apache 0.22.
 */
public class Apache22HadoopShim extends CommonHadoopShim {
  @Override
  public long getNumMapOutputRecords(Job job)
      throws IOException, InterruptedException {
    return job.getCounters().findCounter(
        TaskCounter.MAP_OUTPUT_RECORDS).getValue();
  }

  @Override
  public long getNumMapInputRecords(Job job)
      throws IOException, InterruptedException {
    return job.getCounters().findCounter(
        TaskCounter.MAP_INPUT_RECORDS).getValue();
  }

  @Override
  public String getTaskIdProperty() {
    return JobContext.TASK_ID;
  }

  @Override
  public String getJobLocalDirProperty() {
    return JobContext.JOB_LOCAL_DIR;
  }

  @Override
  public void setJobNumMaps(Job job, int numMapTasks) {
    job.getConfiguration().setInt(JobContext.NUM_MAPS, numMapTasks);
  }

  @Override
  public int getJobNumMaps(JobContext job) {
    return job.getConfiguration().getInt(JobContext.NUM_MAPS, 1);
  }

  @Override
  public int getConfNumMaps(Configuration conf) {
    return conf.getInt(JobContext.NUM_MAPS, 1);
  }

  @Override
  public void setJobMapSpeculativeExecution(Job job, boolean isEnabled) {
    job.setMapSpeculativeExecution(isEnabled);
  }

  @Override
  public void setJobReduceSpeculativeExecution(Job job, boolean isEnabled) {
    job.setReduceSpeculativeExecution(isEnabled);
  }

  @Override
  public void setJobtrackerAddr(Configuration conf, String addr) {
    conf.set(JTConfig.JT_IPC_ADDRESS, "local");
  }

  private static class MockMapContextWithCommitter
      extends MapContextImpl<Object, Object, Object, Object> {
    private Configuration conf;
    private Path path;

    public MockMapContextWithCommitter(Configuration c, Path p) {
      super(c, new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0),
          null, null, null, new MockReporter(new Counters()), null);

      this.conf = c;
      this.path = p;
    }

    @Override
    public InputSplit getInputSplit() {
      return new FileSplit(new Path(path, "inputFile"), 0, 0, new String[0]);
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      try {
        return new FileOutputCommitter(path, this);
      } catch (IOException ioe) {
        return null;
      }
    }
  }

  @Override
  public MapContext getMapContextForIOPath(Configuration conf, Path p) {
    return new MockMapContextWithCommitter(conf, p);
  }
}
