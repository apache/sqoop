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
package org.apache.sqoop.model;

import org.apache.sqoop.common.SqoopException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata describing framework options for connection and job for each
 * supported operation.
 */
public class MFramework extends MPersistableEntity {

  private final MConnection connection;
  private final Map<MJob.Type, MJob> jobs;

  public MFramework(MConnection connection, List<MJob> jobs) {
    this.connection = connection;
    this.jobs = new HashMap<MJob.Type, MJob>();

    for (MJob job : jobs) {
      MJob.Type type = job.getType();

      if(this.jobs.containsKey(type)) {
        throw new SqoopException(ModelError.MODEL_001, "Duplicate entry for"
          + " job type " + job.getType().name());
      }
      this.jobs.put(type, job);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("framework-");
    sb.append(getPersistenceId()).append(":");
    sb.append(", ").append(connection.toString());
    for(MJob entry: jobs.values()) {
      sb.append(entry.toString());
    }

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MFramework)) {
      return false;
    }

    MFramework mo = (MFramework) other;
    return connection.equals(mo.connection) && jobs.equals(mo.jobs);
  }

  @Override
  public int hashCode() {
    int result = connection.hashCode();

    for(MJob entry: jobs.values()) {
      result = 31 * result + entry.hashCode();
    }

    return result;
  }

  public MConnection getConnection() {
    return connection;
  }

  public Map<MJob.Type, MJob> getJobs() {
    return jobs;
  }

  public MJob getJob(MJob.Type type) {
    return jobs.get(type);
  }
}

