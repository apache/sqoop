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
package org.apache.sqoop.connector.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;

/**
 * Utilities for HDFS.
 */
public class HdfsUtils {

  /**
   * Configures the URI to connect to.
   * @param conf Configuration object to be configured.
   * @param linkConfiguration LinkConfiguration object that
   *                          provides configuration.
   * @return Configuration object.
   */
  public static Configuration configureURI(Configuration conf, LinkConfiguration linkConfiguration) {
    if (linkConfiguration.linkConfig.uri != null) {
      conf.set("fs.default.name", linkConfiguration.linkConfig.uri);
      conf.set("fs.defaultFS", linkConfiguration.linkConfig.uri);
    }

    return conf;
  }
}
