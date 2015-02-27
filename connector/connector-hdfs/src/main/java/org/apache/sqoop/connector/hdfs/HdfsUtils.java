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
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;

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
    Configuration newConf = new Configuration(conf);

    if (linkConfiguration.linkConfig.uri != null) {
      newConf.set("fs.default.name", linkConfiguration.linkConfig.uri);
      newConf.set("fs.defaultFS", linkConfiguration.linkConfig.uri);
    }

    return newConf;
  }

  /**
   * Given the configurations, should data received be customized?
   * @param linkConfiguration Link configuration
   * @param fromJobConfiguration Job configuration
   * @return boolean
   */
  public static boolean hasCustomFormat(LinkConfiguration linkConfiguration, FromJobConfiguration fromJobConfiguration) {
    return Boolean.TRUE.equals(fromJobConfiguration.fromJobConfig.overrideNullValue);
  }

  /**
   * Given the configurations, should data received be customized?
   * @param linkConfiguration Link configuration
   * @param toJobConfiguration Job configuration
   * @return boolean
   */
  public static boolean hasCustomFormat(LinkConfiguration linkConfiguration, ToJobConfiguration toJobConfiguration) {
    return Boolean.TRUE.equals(toJobConfiguration.toJobConfig.overrideNullValue);
  }

  /**
   * Given a String record as provided by an intermediate data format or existing HDFS output
   * format the record according to configuration.
   * @param linkConfiguration Link configuration
   * @param fromJobConfiguration Job configuration
   * @param record Object[] record
   * @return Object[]
   */
  public static Object[] formatRecord(LinkConfiguration linkConfiguration,
                                      FromJobConfiguration fromJobConfiguration,
                                      Object[] record) {
    if (fromJobConfiguration.fromJobConfig.overrideNullValue != null
            && fromJobConfiguration.fromJobConfig.overrideNullValue) {
      for (int i = 0; i < record.length; ++i) {
        if (record[i] != null && record[i].equals(fromJobConfiguration.fromJobConfig.nullValue)) {
          record[i] = null;
        }
      }
    }

    return record;
  }

  /**
   * Given an object array record as provided by an intermediate data format
   * format record according to configuration.
   * @param linkConfiguration Link configuration
   * @param toJobConfiguration Job configuration
   * @param record Record array
   * @return Object[]
   */
  public static Object[] formatRecord(LinkConfiguration linkConfiguration,
                                    ToJobConfiguration toJobConfiguration,
                                    Object[] record) {
    if (toJobConfiguration.toJobConfig.overrideNullValue != null
            && toJobConfiguration.toJobConfig.overrideNullValue) {
      for (int i = 0; i < record.length; ++i) {
        if (record[i] == null) {
          record[i] = toJobConfiguration.toJobConfig.nullValue;
        }
      }
    }

    return record;
  }
}
