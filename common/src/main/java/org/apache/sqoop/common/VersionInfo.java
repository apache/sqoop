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
package org.apache.sqoop.common;

public final class VersionInfo {

  private static Package myPackage;
  private static VersionAnnotation annotation;

  static {
    myPackage = VersionAnnotation.class.getPackage();
    annotation = myPackage.getAnnotation(VersionAnnotation.class);
  }

  private VersionInfo() {
    // Disable explicit object creation
  }

  /**
   * Get the version.
   * @return the version string, eg. "2.0.0"
   */
  public static String getVersion() {
    return annotation != null ? annotation.version() : "Unknown";
  }

  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return annotation != null ? annotation.revision() : "Unknown";
  }

  /**
   * The date that the code was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return annotation != null ? annotation.date() : "Unknown";
  }

  /**
   * The user that compiled the code.
   * @return the username of the user
   */
  public static String getUser() {
    return annotation != null ? annotation.user() : "Unknown";
  }

  /**
   * Get the subversion URL for the root directory.
   * @return the url
   */
  public static String getUrl() {
    return annotation != null ? annotation.url() : "Unknown";
  }

}
