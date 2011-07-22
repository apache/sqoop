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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mrunit.mapreduce.mock.MockReporter;

/**
 * Hadoop Shim for CDH3 (based on 0.20.2).
 */
public class CDH3Shim extends CommonHadoopShim {
  @Override
  public long getNumMapOutputRecords(Job job)
      throws IOException, InterruptedException {
    return job.getCounters().findCounter(
        "org.apache.hadoop.mapred.Task$Counter",
        "MAP_OUTPUT_RECORDS").getValue();
  }

  @Override
  public long getNumMapInputRecords(Job job)
      throws IOException, InterruptedException {
    return job.getCounters().findCounter(
        "org.apache.hadoop.mapred.Task$Counter",
        "MAP_INPUT_RECORDS").getValue();
  }

  @Override
  public String getTaskIdProperty() {
    return "mapred.task.id";
  }

  @Override
  public String getJobLocalDirProperty() {
    return "job.local.dir";
  }

  @Override
  public void setJobNumMaps(Job job, int numMapTasks) {
    job.getConfiguration().setInt("mapred.map.tasks", numMapTasks);
  }

  @Override
  public int getJobNumMaps(JobContext job) {
    return job.getConfiguration().getInt("mapred.map.tasks", 1);
  }

  @Override
  public int getConfNumMaps(Configuration conf) {
    return conf.getInt("mapred.map.tasks", 1);
  }

  @Override
  public void setJobMapSpeculativeExecution(Job job, boolean isEnabled) {
    job.getConfiguration().setBoolean(
        "mapred.map.tasks.speculative.execution", isEnabled);
  }

  @Override
  public void setJobReduceSpeculativeExecution(Job job, boolean isEnabled) {
    job.getConfiguration().setBoolean(
        "mapred.reduce.tasks.speculative.execution", isEnabled);
  }

  @Override
  public void setJobtrackerAddr(Configuration conf, String addr) {
    conf.set("mapred.job.tracker", addr);
  }

  private static class MockMapContextWithCommitter
      extends MapContext<Object, Object, Object, Object> {
    private Path path;
    private Configuration conf;

    public MockMapContextWithCommitter(Configuration c, Path p) {
      super(c, new TaskAttemptID("jt", 0, true, 0, 0),
          null, null, null, new MockReporter(new Counters()), null);

      this.path = p;
      this.conf = c;
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      try {
        return new FileOutputCommitter(path, this);
      } catch (IOException ioe) {
        return null;
      }
    }

    @Override
    public InputSplit getInputSplit() {
      return new FileSplit(new Path(path, "inputFile"), 0, 0, new String[0]);
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }

  @Override
  public MapContext getMapContextForIOPath(Configuration conf, Path p) {
    return new MockMapContextWithCommitter(conf, p);
  }
}
