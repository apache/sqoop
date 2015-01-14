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
package org.apache.sqoop.connector.kite.configuration;

import com.google.common.base.Strings;

public class ConfigUtil {

  /**
   * Returns a dataset uri, including the filesystem location part, if it is
   * provided separated,
   */
  public static String buildDatasetUri(String fsLocation, String uri) {
    if (!Strings.isNullOrEmpty(fsLocation) && !uri.contains("://")) {
      // Add fsLocation after the second colon
      int p = uri.indexOf(":", uri.indexOf(":") + 1);
      return uri.substring(0, p + 1) + "//" + fsLocation + uri.substring(p + 1);
    }
    return uri;
  }

  /**
   * Returns a dataset uri, including the filesystem location part, if it is
   * provided separated,
   */
  public static String buildDatasetUri(LinkConfig linkConfig,
      ToJobConfig toJobConfig) {
    return buildDatasetUri(linkConfig.hdfsHostAndPort, toJobConfig.uri);
  }

  /**
   * Indicates whether the given job is a HBase job.
   */
  public static boolean isHBaseJob(ToJobConfig toJobConfig) {
    return toJobConfig.uri.startsWith("dataset:hbase:");
  }

}