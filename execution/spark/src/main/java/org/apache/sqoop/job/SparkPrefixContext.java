/**
sss * Licensed to the Apache Software Foundation (ASF) under one
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.ImmutableContext;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SparkPrefixContext implements ImmutableContext {

  String prefix;
  private final Map<String, String> options;

  public SparkPrefixContext(Map<String, String> options, String prefix) {
    this.options = options;
    this.prefix = prefix;
  }

  protected Map<String, String> getOptions() {
    return options;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getString(String key) {
    return options.get(prefix + key);
  }

  @Override
  public String getString(String key, String defaultValue) {
    String value = getString(key);
    if (value == null || value.trim().length() == 0) {
      value = defaultValue;
    }
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean getBoolean(String key, boolean defaultValue) {
    String value = getString(key);
    boolean result = defaultValue;
    if (value != null) {
      result = Boolean.valueOf(value);
    }

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLong(String key, long defaultValue) {
    if (!options.containsKey(prefix + key)) {
      return defaultValue;
    }

    String value = options.get(prefix + key);

    return Long.parseLong(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getInt(String key, int defaultValue) {
    if (!options.containsKey(prefix + key)) {
      return defaultValue;
    }

    String value = options.get(prefix + key);

    return Integer.parseInt(value);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    Map<String, String> intermediateMap = new HashMap<String, String>();

    for(Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();

      if(key.startsWith(prefix)) {
        intermediateMap.put(key.replaceFirst(prefix, ""), entry.getValue());
      }
    }

    return intermediateMap.entrySet().iterator();
  }

}
