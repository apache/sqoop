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

import java.sql.Driver;

/**
 * Describes the configs associated with the {@link Driver} for executing sqoop jobs.
 */
public class MDriver extends MPersistableEntity implements MClonable {

  private final MDriverConfig driverConfig;
  private final String version;

  public MDriver(MDriverConfig driverConfig, String version) {
    this.driverConfig = driverConfig;
    this.version = version;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("driver-");
    sb.append(getPersistenceId()).append(":");
    sb.append("version = " + version);
    sb.append(", ").append(driverConfig.toString());

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MDriver)) {
      return false;
    }

    MDriver driver = (MDriver) other;
    return version.equals(driver.getVersion()) &&
        driverConfig.equals(driver.driverConfig);
  }

  @Override
  public int hashCode() {
    int result = driverConfig.hashCode();
    result = 31 * result + version.hashCode();
    return result;
  }

  public MDriverConfig getDriverConfig() {
    return driverConfig;
  }

  @Override
  public MDriver clone(boolean cloneWithValue) {
    cloneWithValue = false;
    MDriver copy = new MDriver(this.driverConfig.clone(cloneWithValue), this.version);
    copy.setPersistenceId(this.getPersistenceId());
    return copy;
  }

  public String getVersion() {
    return version;
  }
}