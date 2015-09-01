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

package org.apache.sqoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

public class HiveConfig {

  public static final Log LOG = LogFactory.getLog(HiveConfig.class.getName());

  public static final String HIVE_CONF_CLASS = "org.apache.hadoop.hive.conf.HiveConf";

  public static final String HIVE_SASL_ENABLED = "hive.metastore.sasl.enabled";

  /**
   * Dynamically create hive configuration object.
   * @param conf
   * @return
   * @throws IOException if instantiate HiveConf failed.
   */
  public static Configuration getHiveConf(Configuration conf) throws IOException {
    try {
      Class HiveConfClass = Class.forName(HIVE_CONF_CLASS);
      return ((Configuration)(HiveConfClass.getConstructor(Configuration.class, Class.class)
          .newInstance(conf, Configuration.class)));
    } catch (ClassNotFoundException ex) {
      LOG.error("Could not load " + HIVE_CONF_CLASS
          + ". Make sure HIVE_CONF_DIR is set correctly.");
      throw new IOException(ex);
    } catch (Exception ex) {
      LOG.error("Could not instantiate HiveConf instance.", ex);
      throw new IOException(ex);
    }
  }

  /**
   * Add hive conf to configuration object without overriding already set properties.
   * @param hiveConf
   * @param conf
   */
  public static void addHiveConfigs(Configuration hiveConf, Configuration conf) {
    for (Map.Entry<String, String> item : hiveConf) {
      conf.setIfUnset(item.getKey(), item.getValue());
    }
  }
}

