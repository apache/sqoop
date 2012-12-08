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

import java.util.List;

/**
 * Connector metadata.
 *
 * Includes unique id that identifies connector in metadata store, unique human
 * readable name, corresponding name and all forms for all supported job types.
 */
public final class MConnector extends MFramework {

  private final String uniqueName;
  private final String className;
  private final String version;

  public MConnector(String uniqueName, String className, String version,
      MConnectionForms connectionForms, List<MJobForms> jobForms) {
    super(connectionForms, jobForms);

    if (uniqueName == null || className == null) {
      throw new NullPointerException();
    }

    this.uniqueName = uniqueName;
    this.className = className;
    this.version = version;
  }

  public String getUniqueName() {
    return uniqueName;
  }

  public String getClassName() {
    return className;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("connector-");
    sb.append(uniqueName).append(":").append(getPersistenceId()).append(":");
    sb.append(className);
    sb.append(", ").append(getConnectionForms().toString());
    for(MJobForms entry: getAllJobsForms().values()) {
      sb.append(entry.toString());
    }

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
    return uniqueName.equals(mc.uniqueName)
        && className.equals(mc.className)
        && version.equals(mc.version)
        && super.equals(other);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + uniqueName.hashCode();
    result = 31 * result + className.hashCode();

    return result;
  }
}
