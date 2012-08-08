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

import java.util.ArrayList;
import java.util.List;

/**
 * Metadata describing framework options for connections and jobs.
 */
public class MFramework extends MPersistableEntity {

  private final List<MForm> connectionForms;
  private final List<MForm> jobForms;

  public MFramework(List<MForm> connectionForms, List<MForm> jobForms) {
    this.connectionForms = new ArrayList<MForm>(connectionForms.size());
    this.connectionForms.addAll(connectionForms);

    this.jobForms = new ArrayList<MForm>(jobForms.size());
    this.jobForms.addAll(jobForms);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("framework-");
    sb.append(getPersistenceId()).append(":");
    sb.append("; conn-forms:").append(connectionForms);
    sb.append("; job-forms:").append(jobForms);

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

    MFramework mc = (MFramework) other;
    return connectionForms.equals(mc.connectionForms)
        && jobForms.equals(mc.jobForms);
  }

  @Override
  public int hashCode() {
    int result = 23;
    for (MForm cmf : connectionForms) {
      result = 31 * result + cmf.hashCode();
    }
    for (MForm jmf : jobForms) {
      result = 31 * result + jmf.hashCode();
    }

    return result;
  }

  public List<MForm> getConnectionForms() {
    return connectionForms;
  }

  public List<MForm> getJobForms() {
    return jobForms;
  }
}
