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

/**
 * Metadata describing framework options for connection and jobForms for each
 * supported jobForms type.
 */
public class MFramework extends MPersistableEntity implements MClonable {

  private final MConnectionForms connectionForms;
  private final MJobForms jobForms;
  String version;

  public MFramework(MConnectionForms connectionForms, MJobForms jobForms,
    String version) {
    this.version = version;
    this.connectionForms = connectionForms;
    this.jobForms = jobForms;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("framework-");
    sb.append(getPersistenceId()).append(":");
    sb.append("version = " + version);
    sb.append(", ").append(connectionForms.toString());
    sb.append(jobForms.toString());

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
      jobForms.equals(mo.jobForms);
  }

  @Override
  public int hashCode() {
    int result = connectionForms.hashCode();
    result = 31 * result + jobForms.hashCode();
    result = 31 * result + version.hashCode();
    return result;
  }

  public MConnectionForms getConnectionForms() {
    return connectionForms;
  }

  public MJobForms getJobForms() {
    return jobForms;
  }

  @Override
  public MFramework clone(boolean cloneWithValue) {
    //Framework never have any values filled
    cloneWithValue = false;
    MFramework copy = new MFramework(this.getConnectionForms().clone(cloneWithValue),
        this.getJobForms().clone(cloneWithValue), this.version);
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

