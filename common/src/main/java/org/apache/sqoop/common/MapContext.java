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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ImmutableContext implementation based on (Hash)Map.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MapContext implements ImmutableContext {

  private final Map<String, String> options;

  public MapContext(Map<String, String> options) {
    this.options = options;
  }

  protected Map<String, String> getOptions() {
    return options;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getString(String key) {
    return options.get(key);
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
    if (!options.containsKey(key)) {
      return defaultValue;
    }

    String value = options.get(key);

    return Long.parseLong(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getInt(String key, int defaultValue) {
    if (!options.containsKey(key)) {
      return defaultValue;
    }

    String value = options.get(key);

    return Integer.parseInt(value);
  }

  /**
   * Return all properties starting with given prefix (without the prefix itself)
   *
   * @param prefix Prefix that we need to search and remove
   * @return ImmutableContext with new sub properties
   */
  public Map<String, String> getNestedProperties(String prefix) {
    Map<String, String> subProps = new HashMap<String, String>();
    for (String key : options.keySet()) {
      if (key.startsWith(prefix)) {
        subProps.put(key.substring(prefix.length()), options.get(key));
      }
    }

    return subProps;
  }

  /**
   * get keys matching the the regex
   *
   * @param regex
   * @return Map<String,String> with matching keys
   */
  public Map<String, String> getValByRegex(String regex) {
    Pattern p = Pattern.compile(regex);

    Map<String, String> result = new HashMap<String, String>();
    Matcher m;

    for (String item : options.keySet()) {
      m = p.matcher(item);
      if (m.find()) { // match
        result.put(item, getString(item));
      }
    }
    return result;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return options.entrySet().iterator();
  }
}
