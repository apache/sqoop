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
package org.apache.sqoop.job;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.common.ImmutableContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of immutable context that is based on Hadoop configuration
 * object. Each context property is prefixed with special prefix and loaded
 * directly.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class PrefixContext implements ImmutableContext {

  Configuration configuration;
  String prefix;

  public PrefixContext(Configuration configuration, String prefix) {
    this.configuration = configuration;
    this.prefix = prefix;
  }

  @Override
  public String getString(String key) {
    return configuration.get(prefix + key);
  }

  @Override
  public String getString(String key, String defaultValue) {
    return configuration.get(prefix + key, defaultValue);
  }

  @Override
  public long getLong(String key, long defaultValue) {
    return configuration.getLong(prefix + key, defaultValue);
  }

  @Override
  public int getInt(String key, int defaultValue) {
    return  configuration.getInt(prefix + key, defaultValue);
  }

  @Override
  public boolean getBoolean(String key, boolean defaultValue) {
    return configuration.getBoolean(prefix + key, defaultValue);
  }

  /*
   * TODO: Use getter methods for retrieval instead of
   * exposing configuration directly.
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /*
   * There is no good way to get iterator from the underlying Configuration object that would
   * filter only the prefixed properties, so we create new Context/Map that contains only the
   * relevant properties.
   */
  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    Map<String, String> intermediateMap = new HashMap<String, String>();
    for(Map.Entry<String, String> entry : configuration) {
      String key = entry.getKey();

      if(key.startsWith(prefix)) {
        intermediateMap.put(key.replaceFirst(prefix, ""), entry.getValue());
      }
    }

    return intermediateMap.entrySet().iterator();
  }
}
