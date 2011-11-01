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

package org.apache.sqoop.metastore;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import com.cloudera.sqoop.metastore.JobData;

/**
 * API that defines how jobs are saved, restored, and manipulated.
 *
 * <p>
 * JobStorage instances may be created and then not used; the
 * JobStorage factory may create additional JobStorage instances
 * that return false from accept() and then discard them. The close()
 * method will only be triggered for a JobStorage if the connect()
 * method is called. Connection should not be triggered by a call to
 * accept().</p>
 */
public abstract class JobStorage extends Configured implements Closeable {

  /**
   * Returns true if the JobStorage system can use the metadata in
   * the descriptor to connect to an underlying storage resource.
   */
  public abstract boolean canAccept(Map<String, String> descriptor);


  /**
   * Opens / connects to the underlying storage resource specified by the
   * descriptor.
   */
  public abstract void open(Map<String, String> descriptor)
      throws IOException;

  /**
   * Given a job name, reconstitute a JobData that contains all
   * configuration information required for the job. Returns null if the
   * job name does not match an available job.
   */
  public abstract JobData read(String jobName)
      throws IOException;

  /**
   * Forget about a saved job.
   */
  public abstract void delete(String jobName) throws IOException;

  /**
   * Given a job name and the data describing a configured job, record the job
   * information to the storage medium.
   */
  public abstract void create(String jobName, JobData data)
      throws IOException;

  /**
   * Given a job name and configured job data, update the underlying resource
   * to match the current job configuration.
   */
  public abstract void update(String jobName, JobData data)
      throws IOException;

  /**
   * Close any resources opened by the JobStorage system.
   */
  public void close() throws IOException {
  }

  /**
   * Enumerate all jobs held in the connected resource.
   */
  public abstract List<String> list() throws IOException;
}

