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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata describing framework options for connection and job for each
 * supported job type.
 */
public class MFramework extends MPersistableEntity implements MClonable {

  private final MConnectionForms connectionForms;
  private final Map<MJob.Type, MJobForms> jobs;
  String version;

  public MFramework(MConnectionForms connectionForms, List<MJobForms> jobForms,
    String version) {
    this.version = version;
    this.connectionForms = connectionForms;
    this.jobs = new HashMap<MJob.Type, MJobForms>();

    for (MJobForms job : jobForms) {
      MJob.Type type = job.getType();

      if(this.jobs.containsKey(type)) {
        throw new SqoopException(ModelError.MODEL_001, "Duplicate entry for"
          + " jobForms type " + job.getType().name());
      }
      this.jobs.put(type, job);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("framework-");
    sb.append(getPersistenceId()).append(":");
    sb.append("version = " + version);
    sb.append(", ").append(connectionForms.toString());
    for(MJobForms entry: jobs.values()) {
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
    return version.equals(mo.getVersion()) &&
      connectionForms.equals(mo.connectionForms) &&
      jobs.equals(mo.jobs);
  }

  @Override
  public int hashCode() {
    int result = connectionForms.hashCode();

    for(MJobForms entry: jobs.values()) {
      result = 31 * result + entry.hashCode();
    }
    result = 31 * result + version.hashCode();
    return result;
  }

  public MConnectionForms getConnectionForms() {
    return connectionForms;
  }

  public Map<MJob.Type, MJobForms> getAllJobsForms() {
    return jobs;
  }

  public MJobForms getJobForms(MJob.Type type) {
    return jobs.get(type);
  }

  @Override
  public MFramework clone(boolean cloneWithValue) {
    //Framework never have any values filled
    cloneWithValue = false;
    List<MJobForms> copyJobForms = null;
    if(this.getAllJobsForms()!=null) {
      copyJobForms = new ArrayList<MJobForms>();
      for(MJobForms entry: this.getAllJobsForms().values()) {
        copyJobForms.add(entry.clone(cloneWithValue));
      }
    }
    MFramework copy = new MFramework(this.getConnectionForms().clone(cloneWithValue),
      copyJobForms, this.version);
    copy.setPersistenceId(this.getPersistenceId());
    return copy;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

}

