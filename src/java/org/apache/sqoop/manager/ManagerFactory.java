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

package org.apache.sqoop.manager;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;

/**
 * Interface for factory classes for ConnManager implementations.
 * ManagerFactories are instantiated by o.a.h.s.ConnFactory and
 * stored in an ordered list. The ConnFactory.getManager() implementation
 * calls the accept() method of each ManagerFactory, in order until
 * one such call returns a non-null ConnManager instance.
 */
public abstract class ManagerFactory {
  @Deprecated
  /** Do not use accept(SqoopOptions). Use accept(JobData) instead. */
  public ConnManager accept(SqoopOptions options) {
    throw new RuntimeException(
        "Deprecated method; override ManagerFactory.accept(JobData)");
  }

  /**
   * Instantiate a ConnManager that can fulfill the database connection
   * requirements of the task specified in the JobData.
   * @param jobData the user-provided arguments that configure this
   * Sqoop job.
   * @return a ConnManager that can connect to the specified database
   * and perform the operations required, or null if this factory cannot
   * find a suitable ConnManager implementation.
   */
  public ConnManager accept(JobData jobData) {
    return accept(jobData.getSqoopOptions());
  }

}

