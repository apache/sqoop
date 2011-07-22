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
package org.apache.hadoop.sqoop.shims;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;

/**
 * In order to be compatible with multiple versions of Hadoop, all parts
 * of the Hadoop interface that are not cross-version compatible are
 * encapsulated in an implementation of this class. Users should use
 * the ShimLoader class as a factory to obtain an implementation of
 * HadoopShims corresponding to the version of Hadoop currently on the
 * classpath.
 */
public abstract class HadoopShim {

  protected HadoopShim() {
  }

  /**
   * @return the number of mapper output records from a job using its counters.
   */
  public abstract long getNumMapOutputRecords(Job job)
      throws IOException, InterruptedException;

  /**
   * @return the number of mapper input records from a job using its counters.
   */
  public abstract long getNumMapInputRecords(Job job)
      throws IOException, InterruptedException;

  /**
   * @return the Configuration property identifying the current task id.
   */
  public abstract String getTaskIdProperty();

  /**
   * @return the Configuration property identifying the job's local dir.
   */
  public abstract String getJobLocalDirProperty();

  /**
   * Set the (hinted) number of map tasks for a job.
   */
  public abstract void setJobNumMaps(Job job, int numMapTasks);

  /**
   * Set the mapper speculative execution property for a job.
   */
  public abstract void setJobMapSpeculativeExecution(Job job, boolean isEnabled);

  /**
   * Sets the Jobtracker address to use for a job.
   */
  public abstract void setJobtrackerAddr(Configuration conf, String addr);

  /**
   * Returns a mock MapContext that has both an OutputCommitter and an
   * InputSplit wired to the specified path.
   * Used for testing LargeObjectLoader.
   */
  public abstract MapContext getMapContextForIOPath(
      Configuration conf, Path p);

  public final static synchronized HadoopShim get() {
    return ShimLoader.getHadoopShim();
  }
}
