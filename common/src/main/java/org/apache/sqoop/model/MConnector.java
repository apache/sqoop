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

public final class MConnector extends MPersistableEntity {

  private final String uniqueName;
  private final String className;
  private final List<MForm> connectionForms;
  private final List<MForm> jobForms;

  public MConnector(String uniqueName, String className,
      List<MForm> connectionForms, List<MForm> jobForms) {
    if (uniqueName == null || className == null) {
      throw new NullPointerException();
    }

    this.uniqueName = uniqueName;
    this.className = className;

    this.connectionForms = new ArrayList<MForm>(connectionForms.size());
    this.connectionForms.addAll(connectionForms);

    this.jobForms = new ArrayList<MForm>(jobForms.size());
    this.jobForms.addAll(jobForms);
  }

  public String getUniqueName() {
    return uniqueName;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("connector-");
    sb.append(uniqueName).append(":").append(getPersistenceId()).append(":");
    sb.append(className).append("; conn-forms:").append(connectionForms);
    sb.append("; job-forms:").append(jobForms);

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MConnector)) {
      return false;
    }

    MConnector mc = (MConnector) other;
    return (uniqueName.equals(mc.uniqueName)
        && className.equals(mc.className))
        && connectionForms.equals(mc.connectionForms)
        && jobForms.equals(mc.jobForms);
  }

  @Override
  public int hashCode() {
    int result = 23;
    result = 31 * result + uniqueName.hashCode();
    result = 31 * result + className.hashCode();
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
