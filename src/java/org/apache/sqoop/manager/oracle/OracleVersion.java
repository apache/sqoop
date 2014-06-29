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

package org.apache.sqoop.manager.oracle;

/**
 * Represents an Oracle version and allows comparing of versions.
 */
public class OracleVersion {
  private int major;
  private int minor;
  private int version;
  private int patch;
  private String banner;

  public OracleVersion(int major, int minor, int version, int patch,
      String banner) {

    this.major = major;
    this.minor = minor;
    this.version = version;
    this.patch = patch;
    this.banner = banner;
  }

  public boolean isGreaterThanOrEqualTo(int otherMajor, int otherMinor,
      int otherVersion, int otherPatch) {

    if (this.major > otherMajor) {
      return true;
    }

    if (this.major == otherMajor && this.minor > otherMinor) {
      return true;
    }

    if (this.major == otherMajor && this.minor == otherMinor
        && this.version > otherVersion) {
      return true;
    }

    if (this.major == otherMajor && this.minor == otherMinor
        && this.version == otherVersion && this.patch >= otherPatch) {
      return true;
    }

    return false;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getVersion() {
    return version;
  }

  public int getPatch() {
    return patch;
  }

  public String getBanner() {
    return banner;
  }
}
