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
import org.kitesdk.data.URIBuilder;

public class ConfigUtil {

  /**
   * Returns a dataset uri, including the filesystem location part, if it is
   * provided separated,
   */
  public static String buildDatasetUri(String authority, String uri) {
    if (!Strings.isNullOrEmpty(authority) && !uri.contains("://")) {
      URIBuilder builder = new URIBuilder(uri);

      String[] parts = authority.split(":");
      if (parts.length > 0) {
        builder.with("auth:host", parts[0]);
      }
      if (parts.length > 1) {
        builder.with("auth:port", parts[1]);
      }

      return builder.build().toString().replaceFirst("view:", "dataset:");
    }

    return uri;
  }

  /**
   * Returns a dataset uri, including the filesystem location part, if it is
   * provided separated,
   */
  public static String buildDatasetUri(LinkConfig linkConfig, String uri) {
    return buildDatasetUri(linkConfig.authority, uri);
  }

  /**
   * Returns a dataset uri, including the filesystem location part, if it is
   * provided separated,
   */
  public static String buildDatasetUri(LinkConfig linkConfig,
      ToJobConfig toJobConfig) {
    return buildDatasetUri(linkConfig.authority, toJobConfig.uri);
  }

  /**
   * Indicates whether the given job is a HBase job.
   */
  public static boolean isHBaseJob(ToJobConfig toJobConfig) {
    return toJobConfig.uri.startsWith("dataset:hbase:");
  }

}