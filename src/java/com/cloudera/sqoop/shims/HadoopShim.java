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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
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
   * Parse arguments in 'args' via the GenericOptionsParser and
   * embed the results in the supplied configuration.
   * @param conf the configuration to populate with generic options.
   * @param args the arguments to process.
   * @return the unused args to be passed to the application itself.
   */
  public abstract String [] parseGenericOptions(Configuration conf,
      String [] args) throws IOException;

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
   * Get the (hinted) number of map tasks for a job.
   */
  public abstract int getJobNumMaps(JobContext job);

  /**
   * Get the (hinted) number of map tasks for a job.
   */
  public abstract int getConfNumMaps(Configuration conf);

  /**
   * Set the mapper speculative execution property for a job.
   */
  public abstract void setJobMapSpeculativeExecution(Job job,
      boolean isEnabled);

  /**
   * Set the reducer speculative execution property for a job.
   */
  public abstract void setJobReduceSpeculativeExecution(Job job,
      boolean isEnabled);

  /**
   * Sets the Jobtracker address to use for a job.
   */
  public abstract void setJobtrackerAddr(Configuration conf, String addr);

  /**
   * Returns the Configuration property identifying a DBWritable to use.
   */
  public abstract String getDbInputClassProperty();

  /**
   * Returns the Configuration property identifying the DB username.
   */
  public abstract String getDbUsernameProperty();

  /**
   * Returns the Configuration property identifying the DB password.
   */
  public abstract String getDbPasswordProperty();

  /**
   * Returns the Configuration property identifying the DB connect string.
   */
  public abstract String getDbUrlProperty();

  /**
   * Returns the Configuration property identifying the DB input table.
   */
  public abstract String getDbInputTableNameProperty();

  /**
   * Returns the Configuration property specifying WHERE conditions for the
   * db table.
   */
  public abstract String getDbInputConditionsProperty();

  /**
   * Returns a mock MapContext that has both an OutputCommitter and an
   * InputSplit wired to the specified path.
   * Used for testing LargeObjectLoader.
   */
  public abstract MapContext getMapContextForIOPath(
      Configuration conf, Path p);

  public static final synchronized HadoopShim get() {
    return ShimLoader.getHadoopShim(null);
  }
}
